package types

object LoadFileType extends Enumeration {
  type LoadFileType = Value

  val CountryPopulation: LoadFileType = Value("COUNTRY-POPULATION")
  val CityPopulation: LoadFileType = Value("CITY-POPULATION")
}
