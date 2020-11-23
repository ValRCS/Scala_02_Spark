import org.apache.spark.sql.SparkSession

val session = SparkSession.builder().appName("test").master("local").getOrCreate()
val textFile = session.read.textFile("C:/temp/sample-text-file.txt")
val linesWithSpark = textFile.filter(line => line.contains("Lorem"))
linesWithSpark.count()