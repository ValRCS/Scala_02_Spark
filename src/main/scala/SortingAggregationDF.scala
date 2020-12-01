import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, expr}

object SortingAggregationDF extends App {
  val session = SparkSession.builder().appName("test").master("local").getOrCreate()
  //  session.conf.set("spark.sql.caseSensitive", true) //makes our sql queries case sensitive

  println(s"Session started on Spark version ${session.version}")
  val fPath = "./src/resources/union2010-2011.csv"
  val df = session.read
    .format("csv")
    .option("inferSchema", "true") //so when this works we do not have to recast the values
    .option("header", true) //will use first row for header
    .load(fPath)
    .withColumn("InvoiceDate", col("InvoiceDate").cast("timestamp"))
  df.printSchema()

  //lets make a column indicated total spent so Quantity * UnitPrice
  val df2 = df.withColumn("Total", expr("ROUND(Quantity * UnitPrice, 2)"))
  df2.show(10)
  val topPurchases = df2.sort(desc("Total"))
  topPurchases.show(10)
  val returns = topPurchases.sort("Total").where(expr("Total < 0"))
  println(returns.count)
//  returns.tail(10).foreach(println)
  //to use show meaning we would have a new dataframe
  returns.sort(desc("Total")).show(10)
  println("We got ",df.rdd.getNumPartitions, "RDD partitions")

  // in Scala
  val collectDF = df2.limit(10)
//  collectDF.take(5) // take works with an Integer count
  collectDF.show() // this prints it out nicely
  collectDF.show(5, false)
  //so this guarantees locality for your data, just careful with not asking too much
  var rowArr = collectDF.collect()

  //stats http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameStatFunctions.html
  //missing data functions
  //http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameNaFunctions.html

  df2.where(col("InvoiceNo").equalTo(536365))
    .select("InvoiceNo", "Description", "Total")
    .show(5, false)
  //alternative using SQL = equality
  df2.where(expr("InvoiceNo = 536365"))
    .select("InvoiceNo", "Description", "Total")
    .show(5, false)

  df2.where(col("Description").contains("POSTAGE")).show(10)
  //TODO check syntax on this line
//  df2.where(expr("Description LIKE `post`")).show(5)
  //TODO get rows with postage using regular expression and replace mail
  import org.apache.spark.sql.functions.regexp_replace
  //recipe on how to check regex to check for multiple values
  val simpleColors = Seq("black", "white", "red", "green", "blue")
  val regexString = simpleColors.map(_.toUpperCase).mkString("|")
  // the | signifies `OR` in regular expression syntax
  df2.select(
    regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
    col("Description")).show(12, truncate = false)


}
