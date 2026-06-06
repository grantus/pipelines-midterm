package cpspark.parser

final case class ParsedTimestamp(
  normalized: String,
  timestampMs: Long,
  day: String
)
