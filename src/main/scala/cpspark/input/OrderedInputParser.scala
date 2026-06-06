package cpspark.input

import cpspark.domain.ParseResult
import cpspark.parser.{ParserConfig, SessionLogParser}
import org.apache.spark.rdd.RDD

object OrderedInputParser {
  def parse(
             lines: RDD[InputLine],
             parserConfig: ParserConfig
           ): RDD[ParseResult] = {
    lines.mapPartitions { partitionLines =>
      parseOrderedPartition(partitionLines, parserConfig)
    }
  }

  private def parseOrderedPartition(
                                     lines: Iterator[InputLine],
                                     parserConfig: ParserConfig
                                   ): Iterator[ParseResult] = {
    val buffer = lines.buffered

    new Iterator[ParseResult] {
      private var currentResults: Iterator[ParseResult] = Iterator.empty

      override def hasNext: Boolean = {
        fillCurrentResults()
        currentResults.hasNext
      }

      override def next(): ParseResult = {
        fillCurrentResults()
        currentResults.next()
      }

      private def fillCurrentResults(): Unit = {
        while (!currentResults.hasNext && buffer.hasNext) {
          val currentInputId = buffer.head.inputId
          val currentSessionId = buffer.head.sessionId

          val sameInputLines = new Iterator[(Long, String)] {
            override def hasNext: Boolean = {
              buffer.hasNext &&
                buffer.head.inputId == currentInputId &&
                buffer.head.sessionId == currentSessionId
            }

            override def next(): (Long, String) = {
              val inputLine = buffer.next()
              inputLine.lineNo -> inputLine.line
            }
          }

          currentResults = SessionLogParser.parseNumberedLines(
            inputId = currentInputId,
            sessionId = currentSessionId,
            lines = sameInputLines,
            config = parserConfig
          )
        }
      }
    }
  }
}
