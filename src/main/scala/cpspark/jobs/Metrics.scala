package cpspark.jobs

import cpspark.domain._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{array_contains, col, count, expr}

final case class SearchRow(
                            inputId: String,
                            sessionId: String,
                            searchId: String,
                            searchSource: String,
                            day: String,
                            timestampMs: Long,
                            timestampSource: String,
                            query: Option[String],
                            parameterCount: Int,
                            foundDocs: Seq[String]
                          )

final case class DocumentOpenRow(
                                  inputId: String,
                                  sessionId: String,
                                  searchId: String,
                                  documentId: String,
                                  day: String,
                                  timestampMs: Long,
                                  timestampSource: String
                                )

object Metrics {
  def toSearchRows(events: RDD[Event])(implicit spark: SparkSession): Dataset[SearchRow] = {
    import spark.implicits._

    val rows = events.flatMap {
      case e: QuickSearch =>
        Some(SearchRow(
          inputId = e.meta.inputId,
          sessionId = e.meta.sessionId,
          searchId = e.searchId,
          searchSource = e.searchSource.code,
          day = e.meta.day,
          timestampMs = e.meta.timestampMs,
          timestampSource = e.meta.timestampSource,
          query = e.query,
          parameterCount = 0,
          foundDocs = e.foundDocs
        ))

      case e: CardSearch =>
        Some(SearchRow(
          inputId = e.meta.inputId,
          sessionId = e.meta.sessionId,
          searchId = e.searchId,
          searchSource = e.searchSource.code,
          day = e.meta.day,
          timestampMs = e.meta.timestampMs,
          timestampSource = e.meta.timestampSource,
          query = None,
          parameterCount = e.parameters.size,
          foundDocs = e.foundDocs
        ))

      case _ =>
        None
    }

    spark.createDataset(rows)
  }

  def toDocumentOpenRows(events: RDD[Event])(implicit spark: SparkSession): Dataset[DocumentOpenRow] = {
    import spark.implicits._

    val rows = events.flatMap {
      case e: DocumentOpened =>
        Some(DocumentOpenRow(
          inputId = e.meta.inputId,
          sessionId = e.meta.sessionId,
          searchId = e.searchId,
          documentId = e.documentId,
          day = e.meta.day,
          timestampMs = e.meta.timestampMs,
          timestampSource = e.meta.timestampSource
        ))

      case _ =>
        None
    }

    spark.createDataset(rows)
  }

  def cardSearchCountForDocument(searches: Dataset[SearchRow], documentId: String): DataFrame = {
    searches
      .filter(col("searchSource") === SearchSource.CardSearch.code)
      .filter(array_contains(col("foundDocs"), documentId))
      .agg(count("*").as("card_search_count"))
  }

  def quickSearchOpeningsByDay(
    searches: Dataset[SearchRow],
    opens: Dataset[DocumentOpenRow]
  ): DataFrame = {
    val quickSearches = searches
      .filter(col("searchSource") === SearchSource.QuickSearch.code)
      .select(
        col("sessionId"),
        col("searchId"),
        col("foundDocs")
      )

    opens
      .join(quickSearches, Seq("sessionId", "searchId"), "inner")
      .filter(expr("array_contains(foundDocs, documentId)"))
      .groupBy(col("day"), col("documentId"))
      .agg(count("*").as("openings"))
      .orderBy(col("day"), col("documentId"))
  }
}
