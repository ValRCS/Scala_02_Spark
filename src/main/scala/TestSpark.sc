import org.apache.spark.sql.SparkSession

val session = SparkSession.builder().appName("test").master("local").getOrCreate()
val filePath = "./src/resources/alice.txt"
//val textFile = session.read.textFile("C:/temp/sample-text-file.txt")
val textFile = session.read.textFile(filePath)
val linesWithSpark = textFile.filter(line => line.contains("Alice"))
linesWithSpark.count()