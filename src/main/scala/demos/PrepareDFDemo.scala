package demos

import libs.LoadFileLib.loadCSVFileToDF
import libs.PrepareDFLib.prepareDF
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import types.LoadFileType
import vars.{DefaultVars, LoadFileVars}

object PrepareDFDemo {

  // Object with the fields of the dataframe (just for a good organization)
  object field {
    val pop2024 = "pop2024"
    val pop2023 = "pop2023"
    val city = "city"
    val country = "country"
    val growthRate = "growthRate"
    val type_ = "type"
    val rank = "rank"
  }

  object newColumn {
    val cityCountry = "cityCountry"
    val popGrowth = "popGrowth"
    val cityPlace = "cityPlace"
    val rankedCityCountry = "rankedCityCountry"
  }

  def demo(spark: SparkSession): Unit = {


    // Load dataframe for demo
    val cityPopulationLoadFileVars = LoadFileVars.getVars(LoadFileType.CityPopulation)

    val dfCityPopulation = loadCSVFileToDF(
      spark,
      cityPopulationLoadFileVars)

    dfCityPopulation.show(DefaultVars.showLines)

    // 1st example transformation
    val dfFirst = firstPrepareDF(dfCityPopulation)
    dfFirst.show(DefaultVars.showLines)

    // 2st example transformation
    val dfSecond = secondPrepareDF(dfFirst)
    dfSecond.show(DefaultVars.showLines)
  }

  private def firstPrepareDF(df: DataFrame): DataFrame = {

    val firstTransformationMapping = Map(
      newColumn.cityCountry -> concat(col(field.city), lit("-"), col(field.country)),
      newColumn.popGrowth   -> expr(s"${field.pop2024} - ${field.pop2023}")
    )

    val firstColsToSelect = Array(
      field.rank, field.pop2023, field.pop2024, field.type_
    )

    prepareDF(df, firstTransformationMapping, Option(firstColsToSelect))
  }

  private def secondPrepareDF(df: DataFrame): DataFrame = {

    val secondTransformationMapping = Map(
      newColumn.cityPlace         -> when(col(field.type_) === "us", lit("USA")).otherwise("World"),
      newColumn.rankedCityCountry -> concat(col(field.rank), lit("//"), col(newColumn.cityCountry))
    )

    val secondColsToSelect =
      df.columns
        .filter(!_.contains(field.rank))
        .filter(!_.contains(field.type_))

    prepareDF(df, secondTransformationMapping, Option(secondColsToSelect))
  }
}
