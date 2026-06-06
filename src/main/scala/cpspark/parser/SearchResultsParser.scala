package cpspark.parser

object SearchResultsParser {
  private val SearchIdPattern = "-?\\d+"

  def parse(line: String): Either[String, (String, Seq[String])] = {
    val tokens = line.split("\\s+").toVector.filter(_.nonEmpty)

    tokens match {
      case searchId +: docs if searchId.matches(SearchIdPattern) =>
        Right(searchId -> docs)

      case Vector() =>
        Left("empty search result line")

      case other =>
        Left(s"first token is not a numeric search id: ${other.head}")
    }
  }

  def looksLikeSearchResults(line: String): Boolean = {
    line.split("\\s+").headOption.exists(_.matches(SearchIdPattern))
  }
}
