package demos

import libs.JoinLib.joinNullSafe
import org.apache.spark.sql.{SQLContext, SparkSession}
import vars.DefaultVars

object JoinNullSafeDemo {
  def demo(spark: SparkSession): Unit = {

    // Start context
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    // 1st dataframe to join
    val dfExample1 = Seq(
      ("A", "01", "Key A01"),
      ("A", "02", "Key A02"),
      ("B", "01", "Key B01"),
      ("B", null, "Key B"),
      ("C", "01", "Key C01")
    ).toDF("key_letter", "digit", "key_description")
    dfExample1.show(DefaultVars.showLines)

    // 2nd dataframe to join
    val dfExample2 = Seq(
      ("A", "01", 1, "Item A01.1"),
      ("A", "01", 2, "Item A01.2"),
      ("A", "02", 1, "Item A02.1"),
      ("B", "01", 1, "Item B01.1"),
      ("B", "01", 2, "Item B01.2"),
      ("B", null, 1, "Item B.1"),
      ("B", null, 2, "Item B.2"),
      ("D", "01", 1, "Item D01.1")
    ).toDF("key_letter", "digit", "item", "item_description")
    dfExample2.show(DefaultVars.showLines)

    // Columns defined for the join
    val columnsToJoin = Array("key_letter", "digit")

    // Example of the result without the null safe function
    val dfJoinWithoutNull = dfExample1.join(dfExample2, columnsToJoin, "inner")
    dfJoinWithoutNull.show(DefaultVars.showLines)

    // Example of the result with the null safe function
    val dfJoinWithNull = joinNullSafe(dfExample1, dfExample2, columnsToJoin, "inner")
    dfJoinWithNull.show(DefaultVars.showLines)
  }
}
