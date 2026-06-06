package cpspark.parser

import cpspark.domain._

object LineParser {
  private val SessionStart = "^SESSION_START\\s+(\\S+)\\s*$".r
  private val SessionEnd = "^SESSION_END\\s+(\\S+)\\s*$".r
  private val QuickSearchStart = "^QS\\s+(\\S+)\\s+\\{(.*)}\\s*$".r
  private val CardSearchStart = "^CARD_SEARCH_START\\s+(\\S+)\\s*$".r
  private val CardSearchEnd = "^CARD_SEARCH_END\\s*$".r
  private val DocumentOpenWithTimestamp = "^DOC_OPEN\\s+(\\S+)\\s+(-?\\d+)\\s+(\\S+)\\s*$".r
  private val DocumentOpenWithoutTimestamp = "^DOC_OPEN\\s+(-?\\d+)\\s+(\\S+)\\s*$".r
  private val CardParameter = "^\\$(\\S+)\\s*(.*)$".r

  def parse(
    rawLine: String,
    lineNo: Long,
    context: ParserContext,
    config: ParserConfig
  ): Either[RejectedLine, Option[LineToken]] = {
    val line = rawLine.trim

    if (line.isEmpty) {
      Right(None)
    } else {
      line match {
        case SessionStart(rawTimestamp) =>
          withMeta(rawLine, lineNo, rawTimestamp, context, config) { meta =>
            SessionStartToken(meta)
          }

        case SessionEnd(rawTimestamp) =>
          withMeta(rawLine, lineNo, rawTimestamp, context, config) { meta =>
            SessionEndToken(meta)
          }

        case QuickSearchStart(rawTimestamp, query) =>
          withMeta(rawLine, lineNo, rawTimestamp, context, config) { meta =>
            QuickSearchStartToken(meta, Option(query.trim).filter(_.nonEmpty))
          }

        case CardSearchStart(rawTimestamp) =>
          withMeta(rawLine, lineNo, rawTimestamp, context, config) { meta =>
            CardSearchStartToken(meta)
          }

        case CardSearchEnd() =>
          Right(Some(CardSearchEndToken(rawLine, lineNo)))

        case DocumentOpenWithTimestamp(rawTimestamp, searchId, documentId) =>
          withMeta(rawLine, lineNo, rawTimestamp, context, config) { meta =>
            DocumentOpenToken(meta, searchId, documentId)
          }

        case DocumentOpenWithoutTimestamp(searchId, documentId) =>
          Right(Some(DocumentOpenWithoutTimestampToken(rawLine, lineNo, searchId, documentId)))

        case CardParameter(id, value) =>
          Right(Some(CardParameterToken(rawLine, lineNo, SearchParameter(id, value.trim))))

        case _ if SearchResultsParser.looksLikeSearchResults(line) =>
          SearchResultsParser.parse(line) match {
            case Right((searchId, docs)) =>
              Right(Some(SearchResultsToken(rawLine, lineNo, searchId, docs)))
            case Left(message) =>
              Left(reject(context, lineNo, rawLine, message))
          }

        case _ =>
          Left(reject(context, lineNo, rawLine, "Unknown line format"))
      }
    }
  }

  private def withMeta(
    rawLine: String,
    lineNo: Long,
    rawTimestamp: String,
    context: ParserContext,
    config: ParserConfig
  )(build: EventMeta => LineToken): Either[RejectedLine, Option[LineToken]] = {
    TimestampParser.parse(rawTimestamp, config.zoneId) match {
      case Right(timestamp) =>
        val meta = EventMeta(
          inputId = context.inputId,
          sessionId = context.sessionId,
          lineNo = lineNo,
          timestamp = timestamp.normalized,
          timestampMs = timestamp.timestampMs,
          day = timestamp.day,
          timestampSource = TimestampSource.Explicit,
          raw = rawLine
        )
        Right(Some(build(meta)))

      case Left(message) =>
        Left(reject(context, lineNo, rawLine, message))
    }
  }

  private def reject(
    context: ParserContext,
    lineNo: Long,
    rawLine: String,
    message: String
  ): RejectedLine = {
    RejectedLine(
      ParseFailure(
        inputId = context.inputId,
        sessionId = context.sessionId,
        lineNo = lineNo,
        raw = rawLine,
        message = message
      )
    )
  }
}
