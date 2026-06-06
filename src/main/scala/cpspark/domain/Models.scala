package cpspark.domain

sealed trait Event {
  def meta: EventMeta
  def eventName: String
}

object TimestampSource {
  val Explicit: String = "EXPLICIT"
  val InferredPrevious: String = "INFERRED_PREVIOUS"
}

final case class EventMeta(
                            inputId: String,
                            sessionId: String,
                            lineNo: Long,
                            timestamp: String,
                            timestampMs: Long,
                            day: String,
                            timestampSource: String,
                            raw: String
                          )

final case class SessionStarted(meta: EventMeta) extends Event {
  override val eventName: String = "SESSION_START"
}

final case class SessionEnded(meta: EventMeta) extends Event {
  override val eventName: String = "SESSION_END"
}

sealed trait SearchSource {
  def code: String
}

object SearchSource {
  case object QuickSearch extends SearchSource {
    override val code: String = "QS"
  }

  case object CardSearch extends SearchSource {
    override val code: String = "CARD_SEARCH"
  }
}

sealed trait Search extends Event {
  def searchId: String
  def foundDocs: Seq[String]
  def searchSource: SearchSource
}

final case class QuickSearch(
  meta: EventMeta,
  searchId: String,
  query: Option[String],
  foundDocs: Seq[String]
) extends Search {
  override val eventName: String = "QS"
  override val searchSource: SearchSource = SearchSource.QuickSearch
}

final case class CardSearch(
  meta: EventMeta,
  searchId: String,
  parameters: Seq[SearchParameter],
  foundDocs: Seq[String]
) extends Search {
  override val eventName: String = "CARD_SEARCH"
  override val searchSource: SearchSource = SearchSource.CardSearch
}

final case class DocumentOpened(
  meta: EventMeta,
  searchId: String,
  documentId: String
) extends Event {
  override val eventName: String = "DOC_OPEN"
}

final case class SearchParameter(id: String, value: String)

final case class ParseFailure(
                               inputId: String,
                               sessionId: String,
                               lineNo: Long,
                               raw: String,
                               message: String
)

sealed trait ParseResult
final case class ParsedEvent(event: Event) extends ParseResult
final case class RejectedLine(error: ParseFailure) extends ParseResult
