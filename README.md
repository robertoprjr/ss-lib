# ss-lib
### A Scala/Spark library of functions

*This is a prototype for an internal project*

*Not as a model, functions for Log and Start was added in this library to have an easy way to test new functions*

---

## loadCSVFileToDF() / Load CSV File

#### Target:
**Have a flexible way to load csv files in a Spark DataFrame**

#### Modules:
- *demos/LoadCSVFileToDFDemo*: to show an execution of the function loadCSVFileToDF() 
- *libs/LoadFileLib*: to execute the Spark command to load the defined file
- *types/LoadFileType*: an enumeration with a strong typification to list, in an abstract way, the files to load
- *vars/LoadFileVars*: an interface/trait to be a model for the variables that the load function will need and a derivative object to be a switch between the abstraction of the file and the real variables to get this file
- *vars/loadfiles/(all files)*: examples of the real variables to get this file encapsulated in a class

#### Explanations:

In a first version, a simple load csv files could be like this:
```scala
// Demo execution
val demo01FilePath: String = "data/popc.csv"

val df = loadCSVFileToDF(
  spark,
  filePath = demo01FilePath,
  headerOn = true
)

// Function
object LoadFileLib {

  def loadCSVFileToDF(spark: SparkSession,
                      filePath: String,
                      headerOn: Boolean = false): DataFrame = {

    val headerOption = "header"
    val df = spark.read.option(headerOption, headerOn).csv(filePath)

    df
  }
}
```
But, if you look it with some criteria, you can find some problems:
- It is hard to define the subject of the data that will be load (should be anything);
- If we need more than the "header" option in the function, we will need to create more parameters;
- If we want to put an option to printSchema() in the function we should create parameters too;

And, of course, the use of this without any encapsulation or structure do not show a problem right now in just an example but, when we will work with a bunch of files, it will be a problem too.

To have a better solution we can apply some concepts of best practices of development, such as:
- ***SOLID***: Applying concepts of SRP (Single Responsibility Principal) and OCP (Open/Close Principal)
- ***Composition***: Even not apply a specific design pattern here because these classes don't have an internal behavior, the implementation was based in composition that is base for some patterns
- ***Clean Code***: Ubiquitous Language for example

In practice, we have:

1. A trait/interface as a model for an encapsulated variable class (and adding some others variables to enrich it):
```scala
trait LoadFileVars {
  val filePath: String
  val options: Map[String, String]
  val nickName: String
  val printSchema: Boolean = false
}
```
*PS: A Map here is used for to put all options offer by the system function*
2. An enumeration with a type to list the files, in an abstract way:
```scala
object LoadFileType extends Enumeration {
  type LoadFileType = Value

  val CountryPopulation: LoadFileType = Value("COUNTRY-POPULATION")
  val CityPopulation: LoadFileType = Value("CITY-POPULATION")
}
```
3. An object to be the selector for the files, using pattern match:
```scala
object LoadFileVars {
  def getVars(loadFileType: LoadFileType): LoadFileVars = loadFileType match {
    case LoadFileType.CountryPopulation => new CountryPopulationLoadFileVars()
    case LoadFileType.CityPopulation => new CityPopulationLoadFileVars()
  }
}
```
4. A class with the variables encapsulated:
```scala
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
```
5. Properly the function can have a raw implementation and an overloaded implementation calling the raw implementation:
```scala
  def loadCSVFileToDF(spark: SparkSession,
                      vars: LoadFileVars): DataFrame = {

    loadCSVFileToDF(spark,
      filePath = vars.filePath,
      optionsMap = vars.options,
      nickName = vars.nickName,
      printSchema = vars.printSchema)
  }

  private def loadCSVFileToDF(spark: SparkSession,
                              filePath: String,
                              optionsMap: Map[String, String],
                              nickName: String,
                              printSchema: Boolean = false) : DataFrame = {

    val dfLoaded = spark.read.options(optionsMap).csv(filePath)

    showInfo(s"File (${nickName} : ${filePath}) loaded...")
    if (printSchema) dfLoaded.printSchema()

    dfLoaded
  }
```
6. And the code could be like that:
```scala
    // Load Countries
    val countryPopulationLoadFileVars = LoadFileVars.getVars(LoadFileType.CountryPopulation)

    val dfCountryPopulation = loadCSVFileToDF(
      spark,
      countryPopulationLoadFileVars)
```
---
## joinNullSafe() / Join with columns preserving null values

#### Target:
**Have a function to make a simple join with null values also**

#### Explanations:
The default for Spark join are to ignore null values but, sometimes, we need to make some joins even with fields that are null in both sides.
It happen more yet in cases of treatment of data.

#### Parameters:
```scala
  /***
   * Simple join function considering null values
   * @param dfLeft
   *  Source dataframe that will be on the left side of the join
   * @param dfRight
   *  Source dataframe that will be on the right side of the join
   * @param columnsToJoin
   *  Array of columns to join (columns with the same name in both sides)
   * @param joinType
   *  Type of join equal the original join: (inner, left, right, full, ...)
   * @return
   *  Joined dataframe
   */

  def joinNullSafe(dfLeft: DataFrame,
                   dfRight: DataFrame,
                   columnsToJoin: Array[String],
                   joinType: String = "inner") : DataFrame = ...
```
#### Results: 
1. Based in the following dataframe in the left side of the join:
```scala
    val dfExample1 = Seq(
      ("A", "01", "Key A01"),
      ("A", "02", "Key A02"),
      ("B", "01", "Key B01"),
      ("B", null, "Key B"),
      ("C", "01", "Key C01")
    ).toDF("key_letter", "digit", "key_description")
```
2. And based in the following dataframe in the right side of the join:
```scala
    val dfExample2 = Seq(
      ("A", "01", 1, "Item A01.1"),
      ("A", "01", 2, "Item A01.2"),
      ("A", "02", 1, "Item A02.1"),
      ("B", "01", 1, "Item B01.1"),
      ("B", "01", 2, "Item B01.2"),
      ("B", null, 1, "Item B.1"),
      ("B", null, 2, "Item B.2"),
      ("D", "01", 1, "Item D01.1")
    ).toDF("key_letter", "digit", "item", "item_description")
```
3. The results for a inner join (without and with nulls) are:

**Without null**:
```
+----------+-----+---------------+----+----------------+
|key_letter|digit|key_description|item|item_description|
+----------+-----+---------------+----+----------------+
|         A|   01|        Key A01|   1|      Item A01.1|
|         A|   01|        Key A01|   2|      Item A01.2|
|         A|   02|        Key A02|   1|      Item A02.1|
|         B|   01|        Key B01|   1|      Item B01.1|
|         B|   01|        Key B01|   2|      Item B01.2|
+----------+-----+---------------+----+----------------+
```
**With null safe**
```
+----------+-----+---------------+----+----------------+
|key_letter|digit|key_description|item|item_description|
+----------+-----+---------------+----+----------------+
|         A|   01|        Key A01|   1|      Item A01.1|
|         A|   01|        Key A01|   2|      Item A01.2|
|         A|   02|        Key A02|   1|      Item A02.1|
|         B|   01|        Key B01|   1|      Item B01.1|
|         B|   01|        Key B01|   2|      Item B01.2|
|         B| null|          Key B|   1|        Item B.1|
|         B| null|          Key B|   2|        Item B.2|
+----------+-----+---------------+----+----------------+
```