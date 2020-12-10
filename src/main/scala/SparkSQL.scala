import SortingAggregationDF.session
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkSQL extends App {
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

  df.createOrReplaceTempView("some_sql_view") //so we need this view to use session.sql

  //so the big idea is that we can use the DataFrame
  // and also SQL type syntax when we create TempView

  session.sql("SELECT Quantity, Description FROM some_sql_view LIMIT 20").show(truncate = false)

  session.sql("SELECT Quantity, Description " +
    "FROM some_sql_view " +
    "WHERE quantity > 10 " +
    "ORDER BY quantity DESC " +
    "LIMIT 20").show(truncate = false)

  session.sql("SELECT Country, SUM(Quantity) as SumQuant, AVG(UnitPrice) " +
    "FROM some_sql_view " +
    "GROUP BY Country " +
    "ORDER BY SumQuant DESC " +
    "LIMIT 20").show(truncate = false)
}
