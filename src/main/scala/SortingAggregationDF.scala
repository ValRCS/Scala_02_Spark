import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

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

  //TODO lets make a column indicated total spent so Quantity * UnitPrice
  val df2 = df.withColumn("Total", expr("ROUND(Quantity * UnitPrice, 2)"))
  df2.show(10)

  //TODO then lets sort by that value and find out the top 10 total purchases
}
