package vars.loadfiles

import vars.LoadFileVars

class FlightsItinerariesLoadFileVars extends LoadFileVars {
  override val nickName: String = "Flights Itineraries"
  override val filePath: String = "d:/data/air/flights_itineraries.csv"
  override val options: Map[String, String] = Map(
    "header" -> "true",
    "delimiter" -> ",",
    "inferSchema" -> "false"
  )
  override val printSchema: Boolean = true

}
