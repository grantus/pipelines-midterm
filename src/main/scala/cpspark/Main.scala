package cpspark

import cpspark.domain.{Event, ParsedEvent, RejectedLine}
import cpspark.input.{FileInput, OrderedInputParser}
import cpspark.jobs.Metrics
import cpspark.parser.ParserConfig
import org.apache.spark.sql.SparkSession

import java.time.ZoneId

final case class AppConfig(
  inputPath: String,
  outputPath: String,
  targetDocumentId: String = "ACC_45616",
  encoding: String = "windows-1251",
  zoneId: String = "Europe/Moscow"
)

object Main {
  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("ConsultantPlusSessionMetrics")
      .getOrCreate()

    try {
      val parserConfig = ParserConfig(ZoneId.of(config.zoneId))

      val inputLines = FileInput.readLines(
        sc = spark.sparkContext,
        inputPath = config.inputPath,
        encoding = config.encoding
      )

      val parsed = OrderedInputParser
        .parse(
          lines = inputLines,
          parserConfig = parserConfig
        )
        .cache()

      val events = parsed.flatMap {
        case ParsedEvent(event: Event) => Some(event)
        case _ => None
      }.cache()

      val rejects = parsed.flatMap {
        case RejectedLine(error) => Some(error)
        case _ => None
      }.cache()

      val eventCount = events.count()
      val rejectCount = rejects.count()

      println(s"Parsed events: $eventCount")
      println(s"Rejected lines: $rejectCount")

      val searches = Metrics.toSearchRows(events).cache()
      val opens = Metrics.toDocumentOpenRows(events).cache()

      val cardSearchCount = Metrics.cardSearchCountForDocument(searches, config.targetDocumentId)
      val quickSearchOpenings = Metrics.quickSearchOpeningsByDay(searches, opens)

      cardSearchCount
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"${config.outputPath}/card_search_count_for_${config.targetDocumentId}")

      quickSearchOpenings
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"${config.outputPath}/quick_search_openings_by_day")

      import spark.implicits._
      spark.createDataset(rejects)
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(s"${config.outputPath}/parse_rejects")
    } finally {
      spark.stop()
    }
  }

  private def parseArgs(args: Array[String]): AppConfig = {
    val params = args.sliding(2, 2).collect {
      case Array(key, value) if key.startsWith("--") => key.drop(2) -> value
    }.toMap

    AppConfig(
      inputPath = required(params, "input"),
      outputPath = required(params, "output"),
      targetDocumentId = params.getOrElse("target-document", "ACC_45616"),
      encoding = params.getOrElse("encoding", "windows-1251"),
      zoneId = params.getOrElse("zone", "Europe/Moscow")
    )
  }

  private def required(params: Map[String, String], name: String): String = {
    params.getOrElse(name, throw new IllegalArgumentException(s"Missing required argument --$name"))
  }
}
