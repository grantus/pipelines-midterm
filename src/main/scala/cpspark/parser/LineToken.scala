package cpspark.parser

import cpspark.domain.{EventMeta, SearchParameter}

sealed trait LineToken {
  def raw: String
  def lineNo: Long
}

final case class SessionStartToken(meta: EventMeta) extends LineToken {
  override val raw: String = meta.raw
  override val lineNo: Long = meta.lineNo
}

final case class SessionEndToken(meta: EventMeta) extends LineToken {
  override val raw: String = meta.raw
  override val lineNo: Long = meta.lineNo
}

final case class QuickSearchStartToken(
  meta: EventMeta,
  query: Option[String]
) extends LineToken {
  override val raw: String = meta.raw
  override val lineNo: Long = meta.lineNo
}

final case class CardSearchStartToken(meta: EventMeta) extends LineToken {
  override val raw: String = meta.raw
  override val lineNo: Long = meta.lineNo
}

final case class CardSearchEndToken(
  raw: String,
  lineNo: Long
) extends LineToken

final case class CardParameterToken(
  raw: String,
  lineNo: Long,
  parameter: SearchParameter
) extends LineToken

final case class SearchResultsToken(
  raw: String,
  lineNo: Long,
  searchId: String,
  foundDocs: Seq[String]
) extends LineToken

final case class DocumentOpenToken(
  meta: EventMeta,
  searchId: String,
  documentId: String
) extends LineToken {
  override val raw: String = meta.raw
  override val lineNo: Long = meta.lineNo
}

final case class DocumentOpenWithoutTimestampToken(
  raw: String,
  lineNo: Long,
  searchId: String,
  documentId: String
) extends LineToken
