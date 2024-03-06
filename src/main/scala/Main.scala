import demos._
import libs.StartLib.buildSparkLocalSession
import vars.DefaultVars

object Main {
  def main(args: Array[String]): Unit = {
    println("--- BEGIN :: Load Session ----")
    val spark = buildSparkLocalSession(DefaultVars.appName, DefaultVars.coreNumbers)
    println("--- END :: Load Session ------")

    println("--- BEGIN :: Main Demo ----")
    LoadCSVFileToDFDemo.demo(spark)
    JoinNullSafeDemo.demo(spark)

    Console.in.read()
    println("--- END :: Main Demo ------")
  }
}