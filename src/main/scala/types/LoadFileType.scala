package types

object LoadFileType extends Enumeration {
  type LoadFileType = Value

  val CountryPopulation: LoadFileType = Value("COUNTRY-POPULATION")
  val CityPopulation: LoadFileType = Value("CITY-POPULATION")
  val CombinedFlights2022: LoadFileType = Value("COMBINED-FLIGHTS-2022")
  val FlightsItineraries: LoadFileType = Value("FLIGHTS-ITINERARIES")
}
