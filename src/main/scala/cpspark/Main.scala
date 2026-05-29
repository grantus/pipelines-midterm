package cpspark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Main {
  private val TargetDoc = "ACC_45616"

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: spark-submit --class cpspark.Main <jar> <input-path> <output-path>")
      System.exit(2)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession.builder()
      .appName("ConsultantPlus metrics")
      .getOrCreate()

    import spark.implicits._

    val parsed = spark.sparkContext.wholeTextFiles(inputPath)
      .flatMap { case (path, content) => EventParser.parseFile(path, content) }
      .cache()

    val events: Dataset[LogEvent] = parsed.collect { case Right(e) => e }.toDS().cache()
    val errors: Dataset[ParseError] = parsed.collect { case Left(e) => e }.toDS()

    val acc45616CardSearchCount = events
      .filter(e => e.eventType == "CARD_SEARCH_START" && e.foundDocs.contains(TargetDoc))
      .count()

    Seq((TargetDoc, acc45616CardSearchCount))
      .toDF("document_id", "card_search_count")
      .coalesce(1)
      .write.mode("overwrite").option("header", "true")
      .csv(s"$outputPath/card_search_count_$TargetDoc")

    val quickSearchOpenings = events.rdd
      .groupBy(_.sessionId)
      .flatMap { case (_, sessionEvents) => openingsAttributedToQuickSearch(sessionEvents.toSeq) }
      .toDS()

    val openingsByDayAndDoc = quickSearchOpenings
      .groupBy(col("day"), col("documentId"))
      .agg(count(lit(1)).as("openings"))
      .orderBy(col("day").asc, col("documentId").asc)

    openingsByDayAndDoc
      .coalesce(1)
      .write.mode("overwrite").option("header", "true")
      .csv(s"$outputPath/quick_search_document_openings_by_day")

    events.groupBy(col("eventType")).count().orderBy(col("eventType"))
      .coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(s"$outputPath/diagnostics_event_type_counts")

    errors.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(s"$outputPath/diagnostics_parse_errors")

    spark.stop()
  }

  def openingsAttributedToQuickSearch(sessionEvents: Seq[LogEvent]): Seq[OpenedFromQuickSearch] = {
    val ordered = sessionEvents.sortBy(e => (e.timestampMs.getOrElse(Long.MaxValue), e.index))
    var searchHistory = Vector.empty[SearchEvent]
    val result = Vector.newBuilder[OpenedFromQuickSearch]

    ordered.foreach { event =>
      event.eventType match {
        case "QS" | "CARD_SEARCH_START" =>
          if (event.foundDocs.nonEmpty) {
            searchHistory :+= SearchEvent(
              index = event.index,
              eventType = event.eventType,
              foundDocs = event.foundDocs.toSet,
              searchId = event.searchId,
              day = event.day
            )
          }

        case "DOC_OPEN" =>
          event.openedDoc.foreach { docId =>
            val matchedBySearchId = event.searchId.flatMap { id =>
              searchHistory.findLast(_.searchId.contains(id))
            }

            val matchedSearch = matchedBySearchId.orElse {
              searchHistory.findLast(_.foundDocs.contains(docId))
            }

            if (matchedSearch.exists(_.eventType == "QS")) {
              val outputDay = event.day
                .orElse(matchedSearch.flatMap(_.day))
                .getOrElse("UNKNOWN_DAY")

              result += OpenedFromQuickSearch(outputDay, docId)
            }
          }

        case _ =>
      }
    }

    result.result()
  }
}
