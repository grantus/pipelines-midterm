package cpspark

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.jdk.CollectionConverters._
import scala.util.Try

object EventParser {
  private val mapper = new ObjectMapper()

  private val KnownEventTypes = Seq("SESSION_START", "SESSION_END", "CARD_SEARCH_START", "DOC_OPEN", "QS")
  private val DocIdRegex = "\\b[A-Z][A-Z0-9]{1,15}_[0-9]+\\b".r
  private val ResultLineRegex = "^(-?\\d+)\\s+(.+)$".r
  private val TextEventRegex = ("^(" + KnownEventTypes.mkString("|") + ")\\b\\s*(.*)$").r

  private val EventTypeKeys = Set("event", "eventtype", "event_type", "type", "name", "eventname", "event_name", "action")
  private val SessionKeys = Set("session", "sessionid", "session_id", "sid", "sessionguid", "session_guid")
  private val TimeKeys = Set("timestamp", "time", "ts", "datetime", "date_time", "eventtime", "event_time", "date")
  private val OpenDocKeys = Set("doc", "docid", "doc_id", "document", "documentid", "document_id", "opened_doc", "opened_document")
  private val ResultPathTokens = Set("doc", "docs", "documents", "document", "results", "result", "found", "founddocs", "found_docs", "items", "list")

  private case class PendingSearch(
                                    eventType: String,
                                    timestampMs: Option[Long],
                                    day: Option[String],
                                    index: Long
                                  )

  private case class PlainParseResult(
                                       output: Option[Either[ParseError, LogEvent]],
                                       nextPending: Option[PendingSearch]
                                     )

  def parse(line: String, index: Long): Either[ParseError, LogEvent] = {
    val trimmed = Option(line).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) Left(ParseError(index, line, "empty line"))
    else {
      parseJson(trimmed, index, sessionIdOverride = None)
        .orElse(parseText(trimmed, index))
        .toRight(ParseError(index, line, "unknown event format"))
    }
  }

  def parseFile(filePath: String, content: String): Seq[Either[ParseError, LogEvent]] = {
    val sessionId = filePath.split("[/\\\\]").lastOption.getOrElse(filePath)
    var pendingSearch = Option.empty[PendingSearch]

    content.split("\\R", -1).toSeq.zipWithIndex.flatMap { case (line, lineIndex) =>
      val index = lineIndex.toLong
      val trimmed = Option(line).map(_.trim).getOrElse("")

      if (trimmed.isEmpty) {
        None
      } else {
        parseJson(trimmed, index, sessionIdOverride = Some(sessionId)) match {
          case Some(event) =>
            pendingSearch = None
            Some(Right(event))

          case None =>
            val parsed = parsePlainLine(trimmed, index, sessionId, pendingSearch)
            pendingSearch = parsed.nextPending
            parsed.output
        }
      }
    }
  }

  private def parseJson(line: String, index: Long, sessionIdOverride: Option[String]): Option[LogEvent] = {
    Try(mapper.readTree(line)).toOption.flatMap { root =>
      val fields = flatten(root)
      val eventType = firstByKey(fields, EventTypeKeys)
        .flatMap(normalizeEventType)

      eventType.map { et =>
        val sessionId = firstByKey(fields, SessionKeys)
          .orElse(sessionIdOverride)
          .getOrElse("__NO_SESSION__")

        val timeText = firstByKey(fields, TimeKeys)
        val timestampMs = timeText.flatMap(parseTimestampMs)
        val day = timestampMs.map(ms => Instant.ofEpochMilli(ms).atZone(ZoneId.systemDefault()).toLocalDate.toString)
          .orElse(timeText.flatMap(parseDay))

        val allDocIds = collectDocIds(root, currentPath = Nil, onlyLikelyResultPaths = true)
        val fallbackDocIds = if (allDocIds.nonEmpty) allDocIds else DocIdRegex.findAllIn(line).toSeq.distinct

        val openedDoc =
          if (et == "DOC_OPEN") {
            firstByKey(fields, OpenDocKeys).flatMap(s => DocIdRegex.findFirstIn(s))
              .orElse(fallbackDocIds.headOption)
          } else None

        val foundDocs =
          if (et == "QS" || et == "CARD_SEARCH_START") fallbackDocIds
          else Seq.empty[String]

        LogEvent(index, sessionId, et, timestampMs, day, foundDocs.distinct, openedDoc, None, line)
      }
    }
  }

  private def parseText(line: String, index: Long): Option[LogEvent] = {
    parsePlainLine(line, index, "__NO_SESSION__", None).output.collect {
      case Right(event) => event
    }
  }

  private def parsePlainLine(
                              line: String,
                              index: Long,
                              sessionId: String,
                              pendingSearch: Option[PendingSearch]
                            ): PlainParseResult = {
    line match {
      case TextEventRegex(eventType, _) =>
        eventType match {
          case "QS" | "CARD_SEARCH_START" =>
            val timeText = extractTextTimestamp(line)
            val timestampMs = timeText.flatMap(parseTimestampMs)
            val day = timestampMs.map(ms => Instant.ofEpochMilli(ms).atZone(ZoneId.systemDefault()).toLocalDate.toString)
              .orElse(timeText.flatMap(parseDay))

            val pending = Some(PendingSearch(eventType, timestampMs, day, index))

            PlainParseResult(None, pending)

          case "DOC_OPEN" =>
            parseDocOpen(line, index, sessionId) match {
              case Some(event) => PlainParseResult(Some(Right(event)), None)
              case None => PlainParseResult(Some(Left(ParseError(index, line, "cannot parse DOC_OPEN"))), None)
            }

          case "SESSION_START" | "SESSION_END" =>
            val timeText = extractTextTimestamp(line)
            val timestampMs = timeText.flatMap(parseTimestampMs)
            val day = timestampMs.map(ms => Instant.ofEpochMilli(ms).atZone(ZoneId.systemDefault()).toLocalDate.toString)
              .orElse(timeText.flatMap(parseDay))

            PlainParseResult(
              Some(Right(LogEvent(index, sessionId, eventType, timestampMs, day, Seq.empty, None, None, line))),
              None
            )

          case _ =>
            PlainParseResult(None, pendingSearch)
        }

      case ResultLineRegex(searchId, rest) =>
        val docs = DocIdRegex.findAllIn(rest).toSeq.distinct

        if (docs.nonEmpty) {
          pendingSearch match {
            case Some(search) =>
              PlainParseResult(
                Some(Right(LogEvent(
                  index = index,
                  sessionId = sessionId,
                  eventType = search.eventType,
                  timestampMs = search.timestampMs,
                  day = search.day,
                  foundDocs = docs,
                  openedDoc = None,
                  searchId = Some(searchId),
                  raw = line
                ))),
                None
              )

            case None =>
              PlainParseResult(None, None)
          }
        } else {
          PlainParseResult(None, pendingSearch)
        }

      case _ =>
        PlainParseResult(None, pendingSearch)
    }
  }

  private def parseDocOpen(line: String, index: Long, sessionId: String): Option[LogEvent] = {
    val tokens = line.split("\\s+").filter(_.nonEmpty)

    if (tokens.length >= 4 && tokens.head == "DOC_OPEN") {
      val timeText = tokens(1)
      val searchId = tokens(2)
      val docId = tokens.drop(3).collectFirst {
        case token if DocIdRegex.pattern.matcher(token).matches() => token
      }

      val timestampMs = parseTimestampMs(timeText)
      val day = timestampMs.map(ms => Instant.ofEpochMilli(ms).atZone(ZoneId.systemDefault()).toLocalDate.toString)
        .orElse(parseDay(timeText))

      docId.map { d =>
        LogEvent(index, sessionId, "DOC_OPEN", timestampMs, day, Seq.empty, Some(d), Some(searchId), line)
      }
    } else if (tokens.length >= 3 && tokens.head == "DOC_OPEN") {
      val searchId = tokens(1)
      val docId = tokens.drop(2).collectFirst {
        case token if DocIdRegex.pattern.matcher(token).matches() => token
      }

      docId.map { d =>
        LogEvent(index, sessionId, "DOC_OPEN", None, None, Seq.empty, Some(d), Some(searchId), line)
      }
    } else {
      None
    }
  }

  private def extractTextTimestamp(line: String): Option[String] = {
    val patterns = Seq(
      "\\d{2}\\.\\d{2}\\.\\d{4}_\\d{2}:\\d{2}:\\d{2}".r,
      "[A-Za-z]{3},_\\d{1,2}_[A-Za-z]{3}_\\d{4}_\\d{2}:\\d{2}:\\d{2}_[+-]\\d{4}".r,
      "\\d{4}-\\d{2}-\\d{2}[T_ ]\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z)?".r,
      "\\d{4}-\\d{2}-\\d{2}".r
    )

    patterns.view.flatMap(_.findFirstIn(line)).headOption
  }

  private def normalizeEventType(value: String): Option[String] = {
    val upper = value.trim.toUpperCase
    KnownEventTypes.find(_ == upper)
  }

  private def flatten(node: JsonNode, prefix: List[String] = Nil): Seq[(String, String)] = {
    if (node == null || node.isNull) Seq.empty
    else if (node.isValueNode) Seq(prefix.lastOption.getOrElse("") -> node.asText())
    else if (node.isObject) node.fields().asScala.toSeq.flatMap { e => flatten(e.getValue, prefix :+ e.getKey) }
    else if (node.isArray) node.elements().asScala.toSeq.flatMap(n => flatten(n, prefix))
    else Seq.empty
  }

  private def firstByKey(fields: Seq[(String, String)], keys: Set[String]): Option[String] =
    fields.collectFirst { case (k, v) if keys.contains(k.toLowerCase.replace("-", "_")) && v.nonEmpty => v }

  private def collectDocIds(node: JsonNode, currentPath: List[String], onlyLikelyResultPaths: Boolean): Seq[String] = {
    if (node == null || node.isNull) Seq.empty
    else if (node.isValueNode) {
      val pathText = currentPath.map(_.toLowerCase).mkString(".")
      val likely = currentPath.exists(k => ResultPathTokens.contains(k.toLowerCase.replace("-", "_"))) ||
        ResultPathTokens.exists(pathText.contains)

      if (!onlyLikelyResultPaths || likely) DocIdRegex.findAllIn(node.asText()).toSeq else Seq.empty
    } else if (node.isObject) {
      node.fields().asScala.toSeq.flatMap(e => collectDocIds(e.getValue, currentPath :+ e.getKey, onlyLikelyResultPaths))
    } else if (node.isArray) {
      node.elements().asScala.toSeq.flatMap(n => collectDocIds(n, currentPath, onlyLikelyResultPaths))
    } else Seq.empty
  }

  private def parseTimestampMs(text: String): Option[Long] = {
    val s = text.trim.stripPrefix("\"").stripSuffix("\"")
    val normalized = s.replace('_', ' ').replace('T', ' ')

    if (s.matches("^[0-9]{13}$")) Some(s.toLong)
    else if (s.matches("^[0-9]{10}$")) Some(s.toLong * 1000L)
    else parseInstant(s).map(_.toEpochMilli)
      .orElse(parseZonedDateTime(normalized).map(_.toInstant.toEpochMilli))
      .orElse(parseLocalDateTime(normalized).map(_.toInstant(ZoneId.systemDefault().getRules.getOffset(Instant.now())).toEpochMilli))
      .orElse(parseDay(s).map(d => LocalDate.parse(d).atStartOfDay(ZoneId.systemDefault()).toInstant.toEpochMilli))
  }

  private def parseInstant(s: String): Option[Instant] =
    Try(Instant.parse(s)).toOption

  private val ZonedDateTimeFormats = Seq(
    DateTimeFormatter.ofPattern("EEE, d MMM yyyy HH:mm:ss Z", Locale.ENGLISH)
  )

  private def parseZonedDateTime(s: String): Option[ZonedDateTime] =
    ZonedDateTimeFormats.view.flatMap(fmt => Try(ZonedDateTime.parse(s, fmt)).toOption).headOption

  private val LocalDateTimeFormats = Seq(
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
    DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"),
    DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
  )

  private def parseLocalDateTime(s: String): Option[LocalDateTime] =
    LocalDateTimeFormats.view.flatMap(fmt => Try(LocalDateTime.parse(s, fmt)).toOption).headOption

  private def parseDay(s: String): Option[String] = {
    "([0-9]{4}-[0-9]{2}-[0-9]{2})".r.findFirstIn(s)
      .orElse("([0-9]{2})\\.([0-9]{2})\\.([0-9]{4})".r.findFirstMatchIn(s).map(m => s"${m.group(3)}-${m.group(2)}-${m.group(1)}"))
  }
}
