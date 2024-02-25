package vars.loadfiles

import vars.LoadFileVars

class CityPopulationLoadFileVars extends LoadFileVars {

  override val nickName: String = "Biggest Cities Population"
  override val filePath: String = "data/pcity.csv"
  override val options: Map[String, String] = Map(
    "header" -> "true"
  )
  override val printSchema: Boolean = true
}

