package cpspark.parser

import cpspark.domain._

import scala.collection.mutable

object EventStreamParser {
  def parse(
             lines: Iterator[RawLogLine],
             context: ParserContext,
             config: ParserConfig = ParserConfig()
           ): Iterator[ParseResult] = {
    new Iterator[ParseResult] {
      private val output = mutable.Queue.empty[ParseResult]
      private var state: ParserState = Idle
      private var lastTimestampMeta: Option[EventMeta] = None
      private var inputFinished = false

      override def hasNext: Boolean = {
        fill()
        output.nonEmpty
      }

      override def next(): ParseResult = {
        fill()
        if (output.isEmpty) {
          throw new NoSuchElementException("next on empty parser iterator")
        }
        output.dequeue()
      }

      private def fill(): Unit = {
        while (output.isEmpty && !inputFinished && lines.hasNext) {
          val inputLine = lines.next()
          val lineNo = inputLine.lineNo
          val rawLine = inputLine.raw

          LineParser.parse(rawLine, lineNo, context, config) match {
            case Right(Some(token)) =>
              val step = consumeToken(token, state, context, lastTimestampMeta)
              state = step.nextState
              output.enqueueAll(step.results)
              lastTimestampMeta = metaFromToken(token).orElse(lastTimestampMeta)

            case Right(None) =>
              ()

            case Left(error) =>
              output.enqueueAll(interruptedStateFailures(state, error.error.lineNo))
              state = Idle
              output.enqueue(error)
          }
        }

        if (output.isEmpty && !inputFinished && !lines.hasNext) {
          inputFinished = true
          output.enqueueAll(finish(state))
          state = Idle
        }
      }
    }
  }

  private sealed trait ParserState
  private case object Idle extends ParserState
  private final case class WaitingQuickSearchResults(
    meta: EventMeta,
    query: Option[String]
  ) extends ParserState
  private final case class ReadingCardSearch(
    meta: EventMeta,
    parameters: Vector[SearchParameter]
  ) extends ParserState
  private final case class WaitingCardSearchResults(
    meta: EventMeta,
    parameters: Vector[SearchParameter]
  ) extends ParserState

  private final case class Step(
    nextState: ParserState,
    results: Vector[ParseResult]
  )

  private def consumeToken(
    token: LineToken,
    state: ParserState,
    context: ParserContext,
    lastTimestampMeta: Option[EventMeta]
  ): Step = {
    state match {
      case Idle =>
        consumeIdle(token, context, lastTimestampMeta)

      case WaitingQuickSearchResults(meta, query) =>
        token match {
          case SearchResultsToken(_, _, searchId, foundDocs) =>
            Step(
              Idle,
              Vector(
                ParsedEvent(
                  QuickSearch(
                    meta = meta,
                    searchId = searchId,
                    query = query,
                    foundDocs = foundDocs
                  )
                )
              )
            )

          case _ =>
            val missing = rejectFromMeta(
              meta,
              s"Expected quick-search result line before line ${token.lineNo}"
            )
            val reprocessed = consumeIdle(token, context, lastTimestampMeta)
            Step(reprocessed.nextState, missing +: reprocessed.results)
        }

      case ReadingCardSearch(meta, parameters) =>
        token match {
          case CardParameterToken(_, _, parameter) =>
            Step(ReadingCardSearch(meta, parameters :+ parameter), Vector.empty)

          case CardSearchEndToken(_, _) =>
            Step(WaitingCardSearchResults(meta, parameters), Vector.empty)

          case _ =>
            val missing = rejectFromMeta(
              meta,
              s"Expected card-search parameter or CARD_SEARCH_END before line ${token.lineNo}"
            )
            val reprocessed = consumeIdle(token, context, lastTimestampMeta)
            Step(reprocessed.nextState, missing +: reprocessed.results)
        }

      case WaitingCardSearchResults(meta, parameters) =>
        token match {
          case SearchResultsToken(_, _, searchId, foundDocs) =>
            Step(
              Idle,
              Vector(
                ParsedEvent(
                  CardSearch(
                    meta = meta,
                    searchId = searchId,
                    parameters = parameters,
                    foundDocs = foundDocs
                  )
                )
              )
            )

          case _ =>
            val missing = rejectFromMeta(
              meta,
              s"Expected card-search result line before line ${token.lineNo}"
            )
            val reprocessed = consumeIdle(token, context, lastTimestampMeta)
            Step(reprocessed.nextState, missing +: reprocessed.results)
        }
    }
  }

  private def consumeIdle(
    token: LineToken,
    context: ParserContext,
    lastTimestampMeta: Option[EventMeta]
  ): Step = {
    token match {
      case SessionStartToken(meta) =>
        Step(Idle, Vector(ParsedEvent(SessionStarted(meta))))

      case SessionEndToken(meta) =>
        Step(Idle, Vector(ParsedEvent(SessionEnded(meta))))

      case QuickSearchStartToken(meta, query) =>
        Step(WaitingQuickSearchResults(meta, query), Vector.empty)

      case CardSearchStartToken(meta) =>
        Step(ReadingCardSearch(meta, Vector.empty), Vector.empty)

      case DocumentOpenToken(meta, searchId, documentId) =>
        Step(
          Idle,
          Vector(
            ParsedEvent(
              DocumentOpened(
                meta = meta,
                searchId = searchId,
                documentId = documentId
              )
            )
          )
        )

      case DocumentOpenWithoutTimestampToken(raw, lineNo, searchId, documentId) =>
        lastTimestampMeta match {
          case Some(previousMeta) =>
            val meta = previousMeta.copy(
              lineNo = lineNo,
              raw = raw,
              timestampSource = TimestampSource.InferredPrevious
            )
            Step(
              Idle,
              Vector(
                ParsedEvent(
                  DocumentOpened(
                    meta = meta,
                    searchId = searchId,
                    documentId = documentId
                  )
                )
              )
            )

          case None =>
            Step(
              Idle,
              Vector(reject(
                context,
                lineNo,
                raw,
                "DOC_OPEN has no timestamp and there is no previous timestamp in the session"
              ))
            )
        }

      case SearchResultsToken(raw, lineNo, _, _) =>
        Step(Idle, Vector(reject(context, lineNo, raw, "Unexpected search result line")))

      case CardParameterToken(raw, lineNo, _) =>
        Step(Idle, Vector(reject(context, lineNo, raw, "Unexpected card-search parameter line")))

      case CardSearchEndToken(raw, lineNo) =>
        Step(Idle, Vector(reject(context, lineNo, raw, "Unexpected CARD_SEARCH_END line")))
    }
  }

  private def finish(state: ParserState): Vector[ParseResult] = {
    state match {
      case Idle =>
        Vector.empty

      case WaitingQuickSearchResults(meta, _) =>
        Vector(rejectFromMeta(meta, "Unexpected end of input: quick-search results are missing"))

      case ReadingCardSearch(meta, _) =>
        Vector(rejectFromMeta(meta, "Unexpected end of input: CARD_SEARCH_END is missing"))

      case WaitingCardSearchResults(meta, _) =>
        Vector(rejectFromMeta(meta, "Unexpected end of input: card-search results are missing"))
    }
  }

  private def interruptedStateFailures(state: ParserState, currentLineNo: Long): Vector[ParseResult] = {
    state match {
      case Idle =>
        Vector.empty

      case WaitingQuickSearchResults(meta, _) =>
        Vector(rejectFromMeta(meta, s"Quick search was interrupted by malformed line $currentLineNo"))

      case ReadingCardSearch(meta, _) =>
        Vector(rejectFromMeta(meta, s"Card search was interrupted by malformed line $currentLineNo"))

      case WaitingCardSearchResults(meta, _) =>
        Vector(rejectFromMeta(meta, s"Card search result was interrupted by malformed line $currentLineNo"))
    }
  }

  private def metaFromToken(token: LineToken): Option[EventMeta] = {
    token match {
      case SessionStartToken(meta) => Some(meta)
      case SessionEndToken(meta) => Some(meta)
      case QuickSearchStartToken(meta, _) => Some(meta)
      case CardSearchStartToken(meta) => Some(meta)
      case DocumentOpenToken(meta, _, _) => Some(meta)
      case _: DocumentOpenWithoutTimestampToken => None
      case _: SearchResultsToken => None
      case _: CardParameterToken => None
      case _: CardSearchEndToken => None
    }
  }

  private def rejectFromMeta(meta: EventMeta, message: String): RejectedLine = {
    RejectedLine(
      ParseFailure(
        inputId = meta.inputId,
        sessionId = meta.sessionId,
        lineNo = meta.lineNo,
        raw = meta.raw,
        message = message
      )
    )
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
