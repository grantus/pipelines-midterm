package cpspark

import org.scalatest.funsuite.AnyFunSuite

class EventParserSpec extends AnyFunSuite {
  test("parses JSON quick search documents") {
    val line = """{"eventType":"QS","sessionId":"s1","timestamp":"2026-05-01T10:00:00Z","query":"tax","documents":["ACC_45616","LAW_1"]}"""
    val event = EventParser.parse(line, 0).toOption.get

    assert(event.eventType == "QS")
    assert(event.sessionId == "s1")
    assert(event.foundDocs.contains("ACC_45616"))
  }

  test("parses text DOC_OPEN with day, search id and document id") {
    val line = "DOC_OPEN 20.02.2020_19:35:18 23924172 ACC_45616"
    val event = EventParser.parse(line, 0).toOption.get

    assert(event.eventType == "DOC_OPEN")
    assert(event.day.contains("2020-02-20"))
    assert(event.searchId.contains("23924172"))
    assert(event.openedDoc.contains("ACC_45616"))
  }

  test("parses multiline text search result after CARD_SEARCH_START") {
    val content =
      """CARD_SEARCH_START 01.05.2020_20:43:27
        |23924172 ACC_45615 LAW_333323 ACC_45616
        |DOC_OPEN 01.05.2020_20:44:00 23924172 ACC_45616
        |""".stripMargin

    val events = EventParser.parseFile("session1", content).collect {
      case Right(event) => event
    }

    val search = events.find(e => e.eventType == "CARD_SEARCH_START" && e.searchId.contains("23924172")).get
    val open = events.find(_.eventType == "DOC_OPEN").get

    assert(search.foundDocs.contains("ACC_45616"))
    assert(search.day.contains("2020-05-01"))
    assert(open.openedDoc.contains("ACC_45616"))
    assert(open.searchId.contains("23924172"))
  }

  test("attributes document opening by search id to matching QS") {
    val events = Seq(
      LogEvent(0, "s1", "SESSION_START", Some(1), Some("2026-05-01"), Nil, None, None, ""),
      LogEvent(1, "s1", "QS", Some(2), Some("2026-05-01"), Seq("ACC_45616", "LAW_1"), None, Some("search1"), ""),
      LogEvent(2, "s1", "DOC_OPEN", Some(3), Some("2026-05-01"), Nil, Some("ACC_45616"), Some("search1"), ""),
      LogEvent(3, "s1", "CARD_SEARCH_START", Some(4), Some("2026-05-01"), Seq("LAW_1"), None, Some("search2"), ""),
      LogEvent(4, "s1", "DOC_OPEN", Some(5), Some("2026-05-01"), Nil, Some("LAW_1"), Some("search2"), "")
    )

    val result = Main.openingsAttributedToQuickSearch(events)

    assert(result == Seq(OpenedFromQuickSearch("2026-05-01", "ACC_45616")))
  }
}
