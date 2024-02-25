package vars.loadfiles

import vars.LoadFileVars

class CountryPopulationLoadFileVars() extends LoadFileVars {
  override val nickName: String = "Countries Population"
  override val filePath: String = "data/popc.csv"
  override val options: Map[String, String] = Map (
    "header" -> "true",
    "delimiter" -> ",",
    "inferSchema" -> "true"
  )
  override val printSchema: Boolean = true
}
