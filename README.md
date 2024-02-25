# ss-lib
### A Scala/Spark library of functions

*This is a prototype for an internal project*

*Not as a model, functions for Log and Start was added in this library to have an easy way to test new functions*

## LoadFile (csv)

#### Target:
**Have a flexible way to load csv files in a Spark DataFrame**

#### Modules:
- *demos/LoadFileDemo*: to show an execution
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

To have a better solution...