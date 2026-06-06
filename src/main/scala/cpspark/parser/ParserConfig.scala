package cpspark.parser

import java.time.ZoneId

final case class ParserConfig(
  zoneId: ZoneId = ZoneId.of("Europe/Moscow")
)
