package demos

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import vars.DefaultVars

import scala.annotation.tailrec

object ArrayFunctionsDemo {
  def demo(spark: SparkSession): Unit = {

    // Start context
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val testData = Seq(
      Row(1, "Key_01", Row(Array("A_OK", "B_OK", "C_OK"), Array("A MATCH B", "A MATCH C", "B MATCH C"))),
      Row(2, "Key_02", Row(Array("A_OK", "B_KO", "C_OK"), Array("A NO MATCH B", "A MATCH C", "B NO MATCH C"))),
      Row(3, "Key_03", Row(Array("A_OK", "B_KO", "C_KO"), Array("A NO MATCH B", "A NO MATCH C", "B NO MATCH C"))),
      Row(4, "Key_04", Row(Array("A_OK", "B_OK", "C_KO"), Array("A MATCH B", "A NO MATCH C", "B NO MATCH C"))),
      Row(5, "Key_05", Row(Array("A_OK", "B_OK", "C_OK"), Array("A MATCH B", "A MATCH C", "B MATCH C"))),
      Row(6, "Key_06", Row(Array("A_OK", "B_KO", "C_OK"), Array("A NO MATCH B", "A MATCH C", "B NO MATCH C"))),
      Row(7, "Key_07", Row(Array("A_OK", "B_KO", "C_KO"), Array("A NO MATCH B", "A NO MATCH C", "B NO MATCH C"))),
      Row(8, "Key_08", Row(Array("A_OK", "B_OK", "C_KO"), Array("A MATCH B", "A NO MATCH C", "B NO MATCH C"))),
      Row(9, "Key_09", Row(Array("A_KO", "B_OK", "C_OK"), Array("A NO MATCH B", "A NO MATCH C", "B MATCH C"))),
      Row(10, "Key_10", Row(Array("A_KO", "B_KO", "C_OK"), Array("A NO MATCH B", "A NO MATCH C", "B NO MATCH C"))),
      Row(11, "Key_11", Row(Array("A_KO", "B_KO", "C_KO"), null)),
      Row(12, "Key_12", Row(Array("A_KO", "B_KO", "C_KO"), null)),
      Row(13, "Key_13", Row(Array("A_KO", "B_KO", "C_OK"), Array("A NO MATCH C", "A MATCH B", "B NO MATCH C"))),
      Row(14, "Key_14", Row(Array("B_KO", "A_OK", "C_OK"), Array("B MATCH C", "A NO MATCH B", "A NO MATCH C"))),
      Row(15, "Key_15", Row(Array("C_OK", "B_KO", "A_OK"), Array("N/A", "B MATCH C", "N/A"))),
      Row(16, "Key_16", Row(Array("B_OK", "A_OK", "C_KO"), Array("B MATCH A", "A NO MATCH C", "A MATCH B"))),
      Row(17, "Key_17", Row(null, null)),
      Row(18, "Key_18", Row(null, null)),
      Row(19, "Key_19", Row(Array(""), Array(""))),
      Row(20, "Key_20", Row(Array.empty[String], Array.empty[String]))
    )

    val testSchema = StructType(List(
      StructField("key", IntegerType, false),
      StructField("name_key", StringType, true),
      StructField("result", StructType(List(
        StructField("valid", ArrayType(StringType), true),
        StructField("match", ArrayType(StringType), true)
      )), true)
    ))

    val rddExample1 = spark.sparkContext.parallelize(testData)
    val dfExample1 =  spark.createDataFrame(rddExample1, testSchema)

    dfExample1.show(DefaultVars.showLines, false)

    // List to looking for
    // Rule: 1st item or 2nd item or...
    val validToFind = Array("C_KO", "A_OK", "")
    val matchToFind = Array("A MATCH B", "A NO MATCH C", "")

    val setForNulls = Array("")
    val fieldsTo: Map[String, Array[String]] = Map("result.valid" -> validToFind,
                                                   "result.match" -> matchToFind).withDefaultValue(Array("N/A"))

    def coln(colName: String) : Column = coalesce(col(colName), lit(setForNulls))

    def rep_dot(name: String) : String = name.replace(".", "_")

    def a_function(prefix: String,
                   colName: String,
                   arrays_function: (Column, Column) => Column,
                   compareTo: Array[String]) : Column = {

      arrays_function(coln(s"$colName"), lit(compareTo)).as(s"${prefix}_${rep_dot(colName)}")
    }

    def addTestArrayFields(schema: StructType,
                           fieldsToTest: Map[String, Array[String]],
                           prefix: String = "",
                           primary: Boolean = true) : Array[Column] = {

      schema.fields.flatMap(actualField => {

        val colName: String = if (prefix == "") actualField.name else s"${prefix}.${actualField.name}"

        actualField.dataType match {
          case st: StructType =>
            Array(col(colName).as(colName)) ++ addTestArrayFields(st, fieldsToTest, colName, false)
          case _ =>
            if (fieldsToTest(colName).sameElements(Array("N/A")) && primary)
              Array(col(colName).as(colName))
            else
              Array(
                a_function("overlap", colName, arrays_overlap, fieldsToTest(colName)),
                a_function("intersect", colName, array_intersect, fieldsToTest(colName)),
                a_function("except", colName, array_except, fieldsToTest(colName)),
                a_function("union", colName, array_union, fieldsToTest(colName))
              )
        }
      })
    }

    val dfExample2 = dfExample1.select(addTestArrayFields(dfExample1.schema, fieldsTo): _*)

    dfExample2.show(DefaultVars.showLines, false)
  }
}
