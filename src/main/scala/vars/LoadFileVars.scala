package vars

import types.LoadFileType
import types.LoadFileType.LoadFileType
import vars.loadfiles._

trait LoadFileVars {
  val filePath: String
  val options: Map[String, String]
  val nickName: String
  val printSchema: Boolean = false
}

object LoadFileVars {
  def getVars(loadFileType: LoadFileType): LoadFileVars = loadFileType match {
    case LoadFileType.CountryPopulation => new CountryPopulationLoadFileVars()
    case LoadFileType.CityPopulation => new CityPopulationLoadFileVars()
    case LoadFileType.CombinedFlights2022 => new CombinedFlights2022LoadFileVars()
    case LoadFileType.FlightsItineraries => new FlightsItinerariesLoadFileVars()
  }
}
