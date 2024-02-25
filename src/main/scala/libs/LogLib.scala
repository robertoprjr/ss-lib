package libs

import types.LogType
import types.LogType.LogType
import vars.DefaultVars

import java.util.Calendar
import java.text.SimpleDateFormat

object LogLib {

  private val infoOn: Boolean = true
  private val infoTag: LogType = LogType.Info

  private val warnOn: Boolean = true
  private val warnTag: LogType = LogType.Warn

  def showInfo(message: String): Unit = {
    printValue(message, infoTag, infoOn)
  }

  def showWarn(message: String): Unit = {
    printValue(message, warnTag, warnOn)
  }

  private def printValue(message: String, tag: LogType, on: Boolean) : Unit = {
    if (on) {
      val dateFormat = new SimpleDateFormat(DefaultVars.dateFormatMask)
      val timeOfNow = Calendar.getInstance().getTime

      println(s"${dateFormat.format(timeOfNow)} ${tag.toString} $message")
    }
  }
}
