package cpspark

final case class LogEvent(
                           index: Long,
                           sessionId: String,
                           eventType: String,
                           timestampMs: Option[Long],
                           day: Option[String],
                           foundDocs: Seq[String],
                           openedDoc: Option[String],
                           searchId: Option[String],
                           raw: String
                         )

final case class ParseError(
                             index: Long,
                             source: String,
                             raw: String,
                             reason: String
                           )

final case class SearchEvent(
                              index: Long,
                              eventType: String,
                              foundDocs: Set[String],
                              searchId: Option[String],
                              day: Option[String]
                            )

final case class OpenedFromQuickSearch(day: String, documentId: String)
