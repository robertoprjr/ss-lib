import demos._
import libs.StartLib.buildSparkLocalSession
import vars.DefaultVars

object Main {
  def main(args: Array[String]): Unit = {
    println("--- BEGIN :: Load Session ----")
    val spark = buildSparkLocalSession(DefaultVars.appName, DefaultVars.coreNumbers)
    println("--- END :: Load Session ------")

    println("--- BEGIN :: Main Demo ----")
    println("01. Load CSV File ")
    println("02. Join Null Safe ")
    println("03. Array Functions ")
    println("04. Broadcast Join ")
    println("05. Prepare DF ")
    println("06. Window Spec and Substitute Option")
    println("00. Exit ")

    val inputDemoExecution = scala.io.StdIn.readLine("Enter the option (2 numbers): ")

    inputDemoExecution match {
      case "01" => LoadCSVFileToDFDemo.demo(spark)
      case "02" => JoinNullSafeDemo.demo(spark)
      case "03" => ArrayFunctionsDemo.demo(spark)
      case "04" => BJoinDemo.demo(spark)
      case "05" => PrepareDFDemo.demo(spark)
      case _ => println("Exiting...")
    }

    println("--- END :: Execution ------")
    println("Press any key to finish")
    Console.in.read()
    println("--- END :: Main Demo ------")
  }
}