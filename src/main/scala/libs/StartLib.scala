package libs

import org.apache.spark.sql.SparkSession
import libs.LogLib.showInfo
import vars.DefaultVars

object StartLib {

  private def getSparkConfigVarsForLocalSession: Map[String, String] = {
    DefaultVars.commonSparkConfigVars
  }

  def buildSparkLocalSession(appName: String,
                               cors: Int = 1): SparkSession = {

    val sparkBuilder: SparkSession.Builder = SparkSession.builder()
      .master(s"local[$cors]")
      .appName(appName)

    for (configVar <- getSparkConfigVarsForLocalSession) {
      sparkBuilder.config(configVar._1, configVar._2)
    }

    val spark = sparkBuilder.getOrCreate()
    showInfo(s"Spark app ${spark.conf.get("spark.app.name")} started...")

    spark
  }
}
