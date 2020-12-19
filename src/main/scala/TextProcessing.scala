import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.functions.{asc, desc}

object TextProcessing extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")
  val sales = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("./src/resources/retail-data/by-day/*.csv") //we took multiple csv files and loaded them together into a single DF
    .coalesce(5)
    .where("Description IS NOT NULL")
//    .sort("Description","Quantity")



  sales.show(10, truncate = false)

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

  //Another way of using the RegexTokenizer is to use it to output values matching the provided
  //pattern instead of using it as a gap. We do this by setting the gaps parameter to false. Doing this
  //with a space as a pattern returns all the spaces, which is not too useful, but if we made our
  //pattern capture individual words, we could return those
  import org.apache.spark.ml.feature.RegexTokenizer
  val wordTokenizer = new RegexTokenizer()
    .setInputCol("Description")
    .setOutputCol("DescOut")
    .setPattern("\\w+") //we need to double escape \ because it is used in regex
//    .setPattern("\\w*\\d+\\w*") //we need to double escape \ because it is used in regex
    .setGaps(false) //by using false we make the regex capture the patterns
    .setToLowercase(true)

  val tokenized2 = wordTokenizer.transform(sales.select("Description"))
  tokenized2.show(truncate = false)

  //A common task after tokenization is to filter stop words, common words that are not relevant in
  //many kinds of analysis and should thus be removed. Frequently occurring stop words in English
  //include “the,” “and,” and “but.” Spark contains a list of default stop words you can see by calling
  //the following method, which can be made case insensitive if necessary (as of Spark 2.2,
  //supported languages for stopwords are “danish,” “dutch,” “english,” “finnish,” “french,”
  //“german,” “hungarian,” “italian,” “norwegian,” “portuguese,” “russian,” “spanish,” “swedish,”
  //and “turkish”):
  val englishStopWords = StopWordsRemover
    .loadDefaultStopWords("english")

  //TODO val myStopWords = add two words to englishStopWords Array


  val stops = new StopWordsRemover()
    .setStopWords(englishStopWords)
    .setInputCol("DescOut")
    .setOutputCol("NoStopWords")
  stops.transform(tokenized2).show(truncate=false)



}
