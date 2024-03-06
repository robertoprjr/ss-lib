package demos

import libs.LoadFileLib._
import org.apache.spark.sql.SparkSession
import types.LoadFileType
import vars.{DefaultVars, LoadFileVars}

object LoadCSVFileToDFDemo {

  def demo(spark: SparkSession): Unit = {

    // Load Countries
    val countryPopulationLoadFileVars = LoadFileVars.getVars(LoadFileType.CountryPopulation)

    val dfCountryPopulation = loadCSVFileToDF(
      spark,
      countryPopulationLoadFileVars)

    dfCountryPopulation.show(DefaultVars.showLines)

    // Load Cities
    val cityPopulationLoadFileVars = LoadFileVars.getVars(LoadFileType.CityPopulation)

    val dfCityPopulation = loadCSVFileToDF(
      spark,
      cityPopulationLoadFileVars)

    dfCityPopulation.show(DefaultVars.showLines)

  }
}
