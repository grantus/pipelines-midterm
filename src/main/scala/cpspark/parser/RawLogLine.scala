package cpspark.parser

final case class RawLogLine(
                             lineNo: Long,
                             raw: String
                           )