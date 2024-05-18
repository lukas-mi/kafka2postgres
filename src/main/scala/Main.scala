import org.apache.kafka.common.serialization.StringSerializer

object Main {
  def main(args: Array[String]): Unit = {
    println(classOf[StringSerializer].getName)
    println(classOf[StringSerializer].getSimpleName)
    println(classOf[StringSerializer].getPackageName)
    println(classOf[StringSerializer].getCanonicalName)
  }
}