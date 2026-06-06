package cpspark.parser

import cpspark.domain._
import org.scalatest.funsuite.AnyFunSuite

class ParserSpec extends AnyFunSuite {
  private val context = ParserContext("file:/tmp/4729", "4729")

  test("parses quick search with English zoned timestamp") {
    val records = Parser.parse(
      Iterator(
        "QS Mon,_24_Aug_2020_05:11:58_+0300 {итоговая аттестация спо}",
        "6813530 LAW_177896 PAP_4810 ACC_45616"
      ),
      context
    ).toVector

    val search = records.collect { case ParsedEvent(e: QuickSearch) => e }.head

    assert(search.searchId == "6813530")
    assert(search.query.contains("итоговая аттестация спо"))
    assert(search.meta.timestampSource == TimestampSource.Explicit)
    assert(search.foundDocs.contains("ACC_45616"))
    assert(search.meta.day == "2020-08-24")
    assert(records.collect { case RejectedLine(error) => error }.isEmpty)
  }

  test("parses card search with English zoned timestamp") {
    val records = Parser.parse(
      Iterator(
        "CARD_SEARCH_START Wed,_4_Mar_2020_23:50:13_+0300",
        "$134 коэффициент платной деятельности",
        "CARD_SEARCH_END ",
        "259430843 PKBO_30185 ACC_45616"
      ),
      context
    ).toVector

    val search = records.collect { case ParsedEvent(e: CardSearch) => e }.head

    assert(search.searchId == "259430843")
    assert(search.meta.timestampSource == TimestampSource.Explicit)
    assert(search.parameters == Seq(SearchParameter("134", "коэффициент платной деятельности")))
    assert(search.foundDocs.contains("ACC_45616"))
    assert(search.meta.day == "2020-03-04")
    assert(records.collect { case RejectedLine(error) => error }.isEmpty)
  }

  test("parses DOC_OPEN without timestamp using previous timestamp in the same session") {
    val records = Parser.parse(
      Iterator(
        "QS Sat,_28_Nov_2020_17:09:59_+0300 {образец доверенности в ифнс}",
        "174704474 DOF_77258 PBI_256412",
        "DOC_OPEN  174704474 DOF_77258"
      ),
      context
    ).toVector

    val open = records.collect { case ParsedEvent(e: DocumentOpened) => e }.head

    assert(open.searchId == "174704474")
    assert(open.documentId == "DOF_77258")
    assert(open.meta.day == "2020-11-28")
    assert(open.meta.raw == "DOC_OPEN  174704474 DOF_77258")
    assert(open.meta.timestampSource == TimestampSource.InferredPrevious)
    assert(records.collect { case RejectedLine(error) => error }.isEmpty)
  }

  test("rejects DOC_OPEN without timestamp when no previous timestamp exists") {
    val records = Parser.parse(
      Iterator("DOC_OPEN  174704474 DOF_77258"),
      context
    ).toVector

    val reject = records.collect { case RejectedLine(error) => error }.head

    assert(reject.message.contains("DOC_OPEN has no timestamp"))
  }

  test("parses card search with empty result list as valid event") {
    val records = Parser.parse(
      lines = Iterator(
        "CARD_SEARCH_START 21.07.2020_02:32:49",
        "$134 some parameter",
        "CARD_SEARCH_END",
        "20199305 "
      ),
      context = ParserContext("test-file", "session-1")
    ).toVector

    val events = records.collect {
      case ParsedEvent(event) => event
    }

    val rejects = records.collect {
      case RejectedLine(error) => error
    }

    assert(rejects.isEmpty)

    val cardSearch = events.collect {
      case e: CardSearch => e
    }.head

    assert(cardSearch.searchId == "20199305")
    assert(cardSearch.foundDocs.isEmpty)
  }

  test("parses quick search with empty result list as valid event") {
    val records = Parser.parse(
      lines = Iterator(
        "QS 21.07.2020_02:32:49 {nonexistent query}",
        "123456 "
      ),
      context = ParserContext("test-file", "session-1")
    ).toVector

    val events = records.collect {
      case ParsedEvent(event) => event
    }

    val rejects = records.collect {
      case RejectedLine(error) => error
    }

    assert(rejects.isEmpty)

    val quickSearch = events.collect {
      case e: QuickSearch => e
    }.head

    assert(quickSearch.searchId == "123456")
    assert(quickSearch.query.contains("nonexistent query"))
    assert(quickSearch.foundDocs.isEmpty)
  }
}
