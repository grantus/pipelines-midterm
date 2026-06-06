package cpspark.parser

import cpspark.domain.ParseResult

object SessionLogParser {
  def parseUnnumberedLines(
                  inputId: String,
                  sessionId: String,
                  lines: Iterator[String],
                  config: ParserConfig = ParserConfig()
                ): Iterator[ParseResult] = {
    val numberedLines = lines.zipWithIndex.map { case (line, index) =>
      RawLogLine(
        lineNo = index.toLong + 1L,
        raw = line
      )
    }

    parseRawLines(
      inputId = inputId,
      sessionId = sessionId,
      lines = numberedLines,
      config = config
    )
  }

  def parseNumberedLines(
                          inputId: String,
                          sessionId: String,
                          lines: Iterator[(Long, String)],
                          config: ParserConfig = ParserConfig()
                        ): Iterator[ParseResult] = {
    val rawLines = lines.map { case (lineNo, raw) =>
      RawLogLine(
        lineNo = lineNo,
        raw = raw
      )
    }

    parseRawLines(
      inputId = inputId,
      sessionId = sessionId,
      lines = rawLines,
      config = config
    )
  }

  def parseRawLines(
                     inputId: String,
                     sessionId: String,
                     lines: Iterator[RawLogLine],
                     config: ParserConfig = ParserConfig()
                   ): Iterator[ParseResult] = {
    Parser.parseRawLines(
      lines = lines,
      context = ParserContext(inputId = inputId, sessionId = sessionId),
      config = config
    )
  }
}
