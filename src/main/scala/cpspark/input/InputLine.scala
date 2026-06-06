package cpspark.input

final case class InputLine(inputId: String,
                           sessionId: String,
                           lineNo: Long,
                           line: String
                          )
