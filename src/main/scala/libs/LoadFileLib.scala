package libs

import libs.LogLib._
import org.apache.spark.sql.{DataFrame, SparkSession}
import vars.LoadFileVars

object LoadFileLib {

  /***
   * Function for load csv files using var structure defined
   * @param spark
   *  SparkSession created
   * @param vars
   *  Object with the file's vars to load the file
   * @return
   *  A dataframe with the file's content
   */
  def loadCSVFileToDF(spark: SparkSession,
                      vars: LoadFileVars): DataFrame = {

    loadCSVFileToDF(spark,
      filePath = vars.filePath,
      optionsMap = vars.options,
      nickName = vars.nickName,
      printSchema = vars.printSchema)
  }

  /***
   * Generic function for load csv files
   * @param spark
   *  SparkSession created
   * @param filePath
   *  Path's file to load
   * @param optionsMap
   *  Mapping of the options for the read function
   * @param nickName
   *  A nickname of the file
   * @param printSchema
   *  If the dataframe after load will be print or not
   * @return
   *  A dataframe with the file's content
   */
  private def loadCSVFileToDF(spark: SparkSession,
                              filePath: String,
                              optionsMap: Map[String, String],
                              nickName: String,
                              printSchema: Boolean = false) : DataFrame = {

    val dfLoaded : DataFrame = spark.read.options(optionsMap).csv(filePath)

    showInfo(s"File (${nickName} : ${filePath}) loaded...")
    if (printSchema) dfLoaded.printSchema()

    dfLoaded
  }
}
