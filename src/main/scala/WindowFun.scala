import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

object WindowFun extends App {
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
    .withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
  df.printSchema()
  df.show(5, false)

}
