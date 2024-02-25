package vars

object DefaultVars {

  val dateFormatMask: String = "yy/MM/dd hh:mm:ss"
  val appName: String = "ss-util.com"
  val coreNumbers: Int = 4
  val showLines: Int = 50

  val commonSparkConfigVars: Map[String, String] = Map(
    "spark.sql.shuffle.partitions" -> "30",
    "spark.shuffle.compress" -> "true",
    "spark.eventLog.enabled" -> "false"
  )
}
