package cpspark.parser

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.util.Locale
import scala.util.Try

object TimestampParser {
  private val LocalFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
  private val ZonedEnglishFormatter = DateTimeFormatter.ofPattern(
    "EEE,_d_MMM_yyyy_HH:mm:ss_Z",
    Locale.ENGLISH
  )

  def parse(raw: String, zoneId: ZoneId): Either[String, ParsedTimestamp] = {
    parseLocal(raw, zoneId)
      .orElse(parseZonedEnglish(raw))
      .toRight(s"Invalid timestamp: $raw")
  }

  private def parseLocal(raw: String, zoneId: ZoneId): Option[ParsedTimestamp] = {
    Try(LocalDateTime.parse(raw, LocalFormatter)).toOption.map { dateTime =>
      ParsedTimestamp(
        normalized = dateTime.toString,
        timestampMs = dateTime.atZone(zoneId).toInstant.toEpochMilli,
        day = dateTime.toLocalDate.toString
      )
    }
  }

  private def parseZonedEnglish(raw: String): Option[ParsedTimestamp] = {
    Try(ZonedDateTime.parse(raw, ZonedEnglishFormatter)).toOption.map { dateTime =>
      ParsedTimestamp(
        normalized = dateTime.toLocalDateTime.toString,
        timestampMs = dateTime.toInstant.toEpochMilli,
        day = dateTime.toLocalDate.toString
      )
    }
  }
}
