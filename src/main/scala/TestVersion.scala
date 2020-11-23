object TestVersion extends App{
  println(s"Testing Scala version: ${util.Properties.versionNumberString}")

  import org.apache.spark.sql.SparkSession

  val session = SparkSession.builder().appName("test").master("local").getOrCreate()
  val filePath = "./src/resources/alice.txt"
//  val filePath = "C:/temp/sample-text-file.txt")
  val textFile = session.read.textFile(filePath)
  val linesWithSpark = textFile.filter(line => line.contains("Alice"))
  println(linesWithSpark.count())

}
