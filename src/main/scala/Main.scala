import demos._

object Main {
  def main(args: Array[String]): Unit = {
    println("--- BEGIN :: Main Execute ----")
    LoadFileDemo.show()

    Console.in.read()
    println("--- END :: Main Execute ------")
  }
}