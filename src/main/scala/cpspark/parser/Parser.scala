package cpspark.parser

import cpspark.domain.ParseResult

object Parser {
  def parse(
             lines: Iterator[String],
             context: ParserContext,
             config: ParserConfig = ParserConfig()
           ): Iterator[ParseResult] = {
    val numberedLines = lines.zipWithIndex.map { case (line, index) =>
      RawLogLine(
        lineNo = index.toLong + 1L,
        raw = line
      )
    }

    parseRawLines(
      lines = numberedLines,
      context = context,
      config = config
    )
  }

  def parseRawLines(
                     lines: Iterator[RawLogLine],
                     context: ParserContext,
                     config: ParserConfig = ParserConfig()
                   ): Iterator[ParseResult] = {
    EventStreamParser.parse(
      lines = lines,
      context = context,
      config = config
    )
  }
}
