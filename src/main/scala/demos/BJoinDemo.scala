package demos

import libs.LoadFileLib.loadCSVFileToDF
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col}
import types.LoadFileType
import vars.{DefaultVars, LoadFileVars}

object BJoinDemo {

  def demo(spark: SparkSession): Unit = {

    // Init
    val sc = spark.sparkContext
    val numbers = sc.parallelize(1 to 100000)

    println("--------- PARTITIONS -------------")
    println(numbers.partitions.length)
    println("----------------------------------")

    object field {
      val FlightDate = "FlightDate"
      val Airline = "Airline"
      val Origin = "Origin"
      val Dest = "Dest"
    }

    val aliasSource = "source"
    val aliasBJoin = "bjoin"
    val aliasSuffix = "suffix"

    val suffixOne = "_1"

    // Load Combined Flights 2022
    val combinedFlights2022LoadFileVars = LoadFileVars.getVars(LoadFileType.CombinedFlights2022)

    val dfCombinedFlights2022 = loadCSVFileToDF(
      spark,
      combinedFlights2022LoadFileVars).repartition(4)

    // General solution
    val fieldsOriginToBJoinList = List(
      field.FlightDate, field.Airline, field.Origin, field.Dest
    )

    def withSuffix(colName: String, suffix: String = suffixOne) : String = s"${colName}${suffix}"

    def colSuffix(colName: String, suffix: String) : Column =
      col(colName).as(withSuffix(colName, suffix))

    val dfCFSuffix = dfCombinedFlights2022
      .select(dfCombinedFlights2022.columns.map(colName => colSuffix(colName, suffixOne)): _*)

    val joinCFKey: Column =
      col(field.FlightDate) === col(withSuffix(field.FlightDate, suffixOne)) &&
        col(field.Airline) === col(withSuffix(field.Airline, suffixOne)) &&
        col(field.Origin) === col(withSuffix(field.Origin, suffixOne)) &&
        col(field.Dest) === col(withSuffix(field.Dest, suffixOne))


    // BJoin Solution
    /*
    val dfCFSmall = dfCFSuffix
      .select(fieldsOriginToBJoinList.map(colName => col(withSuffix(colName, suffixOne))): _*)

    val dfCFBJoin = dfCombinedFlights2022.as(aliasSource)
      .join(broadcast(dfCFSmall.as(aliasBJoin)), joinCFKey, "inner")

    val dfCFBJoinResult = dfCFBJoin.as(aliasSource)
      .join(dfCFSuffix.as(aliasSuffix),
        fieldsOriginToBJoinList.map(colName => withSuffix(colName, suffixOne)),
        "inner")
    */


    // SJoin Solution
    val dfCFSJoinResult = dfCombinedFlights2022.as(aliasSource)
      .join(dfCFSuffix.as(aliasSuffix),
        joinCFKey,
        "inner")



    // Load Flights Itineraries
    /*
    val flightsItinerariesLoadFileVars = LoadFileVars.getVars(LoadFileType.FlightsItineraries)

    val dfFlightsItineraries = loadCSVFileToDF(
      spark,
      flightsItinerariesLoadFileVars)

    dfFlightsItineraries.select(
      List("legId", "startingAirport", "destinationAirport", "fareBasisCode", "segmentsDistance").map(col): _*
    )
    dfFlightsItineraries.show(DefaultVars.showLines)
    */

    // Show
    dfCFSJoinResult.show(DefaultVars.showLines)
  }
}
