package libs

import org.apache.spark.sql.functions.{coalesce, col}
import org.apache.spark.sql.{Column, DataFrame}

object JoinLib {

  /***
   * Simple join function considering null values
   * @param dfLeft
   *  Source dataframe that will be on the left side of the join
   * @param dfRight
   *  Source dataframe that will be on the right side of the join
   * @param columnsToJoin
   *  Array of columns to join (columns with the same name in both sides)
   * @param joinType
   *  Type of join equal the original join: (inner, left, right, full, ...)
   * @return
   *  Joined dataframe
   */
  def joinNullSafe(dfLeft: DataFrame,
                   dfRight: DataFrame,
                   columnsToJoin: Array[String],
                   joinType: String = "inner") : DataFrame = {

    val usedSuffix: String = "_nullsafe"

    val dfRightRenamed : DataFrame =
      dfRight.select(dfRight.columns.map { columnName =>
        if (columnsToJoin.contains(columnName))
          col(columnName).as(s"$columnName$usedSuffix")
        else
          col(columnName)
      }: _*)

    val columnExpr : Column =
      dfLeft(columnsToJoin.head) <=> dfRightRenamed(s"${columnsToJoin.head}$usedSuffix")

    val fullColumnExpr : Column = columnsToJoin.tail.foldLeft(columnExpr) {
      (columnExpr, columnName) =>
        columnExpr && dfLeft(columnName) <=> dfRightRenamed(s"$columnName$usedSuffix")
    }

    val dfJoined : DataFrame = dfLeft.join(dfRightRenamed, fullColumnExpr, joinType)

    val dfJoinedCoalesced: DataFrame =
      dfJoined.select(dfJoined.columns.map { columnName =>
        if (columnsToJoin.contains(columnName))
          coalesce(col(columnName), col(s"$columnName$usedSuffix")).as(columnName)
        else
          col(columnName)
      }: _*)

    dfJoinedCoalesced.drop(columnsToJoin.map { columnName =>
      s"$columnName$usedSuffix"
    }: _*)
  }
}
