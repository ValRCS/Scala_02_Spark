import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Tokenizer

object TextProcessing extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")
  val sales = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("./src/resources/retail-data/by-day/*.csv") //we took multiple csv files and loaded them together into a single DF
    .coalesce(5)
    .where("Description IS NOT NULL")

  val tkn = new Tokenizer().setInputCol("Description").setOutputCol("DescOut")
  val tokenized = tkn.transform(sales.select("Description"))
  tokenized.printSchema()
  tokenized.show(false)

  import org.apache.spark.ml.feature.RegexTokenizer
  val rt = new RegexTokenizer()
    .setInputCol("Description")
    .setOutputCol("DescOut")
//    .setPattern("T") // simplest expression probably will not need to split by T though
    .setPattern(" ") // simplest expression probably will not need to split by T though
    .setToLowercase(false)
  rt.transform(sales.select("Description")).show(false)

}
