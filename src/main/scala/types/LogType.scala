package types

object LogType extends Enumeration {
  type LogType = Value

  val Info: LogType = Value("INFO")
  val Warn: LogType = Value("WARN")
}