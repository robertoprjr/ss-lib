package demos

import libs.LoadFileLib._
import libs.StartLib._
import types.LoadFileType
import vars.{DefaultVars, LoadFileVars}

object LoadFileDemo {

  def show(): Unit = {

    val spark = buildSparkLocalSession(DefaultVars.appName, DefaultVars.coreNumbers)
    import spark.implicits._

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
