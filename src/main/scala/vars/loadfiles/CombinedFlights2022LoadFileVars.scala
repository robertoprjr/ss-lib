package vars.loadfiles

import vars.LoadFileVars

class CombinedFlights2022LoadFileVars extends LoadFileVars {
  override val nickName: String = "Combined Flights 2022"
  override val filePath: String = "d:/data/air/combined_flights_2022.csv"
  override val options: Map[String, String] = Map(
    "header" -> "true",
    "delimiter" -> ",",
    "inferSchema" -> "false"
  )
  override val printSchema: Boolean = true
}
