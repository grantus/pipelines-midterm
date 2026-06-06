package cpspark.jobs

import cpspark.domain._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class MetricsSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("MetricsSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("counts only card searches containing target document") {
    implicit val implicitSpark: SparkSession = spark

    val events = eventRdd(
      CardSearch(meta("s1", 1), "search1", Seq.empty, Seq("ACC_45616", "LAW_1")),
      CardSearch(meta("s1", 2), "search2", Seq.empty, Seq("LAW_2")),
      QuickSearch(meta("s1", 3), "search3", None, Seq("ACC_45616"))
    )

    val searches = Metrics.toSearchRows(events)

    val count = Metrics
      .cardSearchCountForDocument(searches, "ACC_45616")
      .collect()
      .head
      .getLong(0)

    assert(count == 1L)
  }

  test("attributes DOC_OPEN to quick search only by sessionId and searchId") {
    implicit val implicitSpark: SparkSession = spark

    val events = eventRdd(
      QuickSearch(meta("s1", 1), "same-search-id", None, Seq("ACC_45616", "LAW_1")),
      DocumentOpened(meta("s1", 2), "same-search-id", "ACC_45616"),

      QuickSearch(meta("s2", 1), "same-search-id", None, Seq("LAW_2")),
      DocumentOpened(meta("s2", 2), "same-search-id", "ACC_45616"),

      QuickSearch(meta("s3", 1), "other-search-id", None, Seq("ACC_45616")),
      DocumentOpened(meta("s3", 2), "same-search-id", "ACC_45616")
    )

    val searches = Metrics.toSearchRows(events)
    val opens = Metrics.toDocumentOpenRows(events)

    val rows = Metrics
      .quickSearchOpeningsByDay(searches, opens)
      .collect()
      .map(row => (
        row.getAs[String]("day"),
        row.getAs[String]("documentId"),
        row.getAs[Long]("openings")
      ))
      .toSet

    assert(rows == Set(("2020-05-01", "ACC_45616", 1L)))
  }

  test("does not attribute document opening when opened document is absent from quick-search results") {
    implicit val implicitSpark: SparkSession = spark

    val events = eventRdd(
      QuickSearch(meta("s1", 1), "search1", None, Seq("LAW_1")),
      DocumentOpened(meta("s1", 2), "search1", "ACC_45616")
    )

    val searches = Metrics.toSearchRows(events)
    val opens = Metrics.toDocumentOpenRows(events)

    val result = Metrics.quickSearchOpeningsByDay(searches, opens).collect()

    assert(result.isEmpty)
  }

  private def eventRdd(events: Event*): RDD[Event] = {
    spark.sparkContext.parallelize(events.toSeq)
  }

  private def meta(sessionId: String, lineNo: Long): EventMeta = {
    EventMeta(
      inputId = s"$sessionId.log",
      sessionId = sessionId,
      lineNo = lineNo,
      timestamp = "2020-05-01T20:43:27",
      timestampMs = 1588355007000L,
      timestampSource = TimestampSource.Explicit,
      day = "2020-05-01",
      raw = "raw"
    )
  }
}
