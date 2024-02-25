package libs

import libs.LogLib._
import org.apache.spark.sql.{DataFrame, SparkSession}
import vars.LoadFileVars

object LoadFileLib {

  def loadCSVFileToDF(spark: SparkSession,
                      vars: LoadFileVars): DataFrame = {

    loadCSVFileToDF(spark,
      filePath = vars.filePath,
      optionsMap = vars.options,
      nickName = vars.nickName,
      printSchema = vars.printSchema)
  }

  def loadCSVFileToDF(spark: SparkSession,
                      filePath: String,
                      optionsMap: Map[String, String],
                      nickName: String,
                      printSchema: Boolean = false) : DataFrame = {

    val df = spark.read.options(optionsMap).csv(filePath)

    showInfo(s"File (${nickName} : ${filePath}) loaded...")
    if (printSchema) df.printSchema()

    df
  }
}
