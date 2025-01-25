package demos

import libs.LoadFileLib.loadCSVFileToDF
import libs.PrepareDFLib.prepareDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import types.LoadFileType
import vars.{DefaultVars, LoadFileVars}

object PrepareDFDemo {

  def demo(spark: SparkSession): Unit = {

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
    }

    // Load dataframe for demo
    val cityPopulationLoadFileVars = LoadFileVars.getVars(LoadFileType.CityPopulation)

    val dfCityPopulation = loadCSVFileToDF(
      spark,
      cityPopulationLoadFileVars)

    dfCityPopulation.show(DefaultVars.showLines)

    // 1st example transformation
    val firstTransformationMapping = Map(
      newColumn.cityCountry -> concat(col(field.city), lit("-") , col(field.country)),
      newColumn.popGrowth   -> expr(s"${field.pop2024} - ${field.pop2023}")
    )

    val firstColsToSelect = Array(
      field.rank, field.pop2023, field.pop2024
    )

    val dfFirst = prepareDF(dfCityPopulation, firstTransformationMapping, Option(firstColsToSelect))
    dfFirst.show(DefaultVars.showLines)
  }
}
