package libs

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}
object PrepareDFLib {

  /***
   * Prepare a dataframe with some transformation by fields mapped
   * Let the code more organized because all the chances in a dataframe will be in a mapped list
   * And it can be a good replacer for function like .withColumn and .withColumnRenamed without make several dfs
   * Helping in customize the return by selecting the fields without a .drop
   * @param df
   * The dataframe with the data
   * @param colsMapping
   * The mapping of the transformation in the dataframe
   * @param colsSelect
   * Optional list of fields wanted in the return
   * IF it is not set, the return will be all fields in the dataframe + the mapped fields
   * @return
   */

  def prepareDF(df: DataFrame,
                colsMapping: Map[String, Column],
                colsSelect: Option[Array[String]] = None) : DataFrame = {

    val colsToSelect = colsSelect match {
      case Some(cols) => Array.concat(cols, colsMapping.keys.toArray).distinct
      case _          => Array.concat(df.columns, colsMapping.keys.toArray).distinct
    }

    def colMapped(colName: String): Column =
      if (colsMapping.contains(colName)) colsMapping(colName).as(colName) else col(colName)

    df.select(colsToSelect.map(colMapped): _*)
  }
}
