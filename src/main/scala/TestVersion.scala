object TestVersion extends App{
  println(s"Testing Scala version: ${util.Properties.versionNumberString}")

  import org.apache.spark.sql.SparkSession

  val session = SparkSession.builder().appName("test").master("local").getOrCreate()
  val filePath = "./src/resources/alice.txt"
//  val filePath = "C:/temp/sample-text-file.txt")
  val textFile = session.read.textFile(filePath)
  val linesWithSpark = textFile.filter(line => line.contains("Alice"))
  println(linesWithSpark.count())
  val lineSize = textFile.rdd.map(line => (line, line.split(" ").size)) //TODO better regex for spliting multiple
  val longestLineTuple = lineSize.reduce((a,b) => if (a._2 > b._2) a else b) //so for loop will not work on RDD
  println(longestLineTuple)
  val wordCounts = textFile.rdd.flatMap(line => line.split(" "))
//    .groupByKey(identity).count()
//
//  val longestLine = textFile.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))
}
