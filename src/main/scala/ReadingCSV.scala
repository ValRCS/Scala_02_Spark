import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{desc, asc}

object ReadingCSV extends App {


  val session = SparkSession.builder().appName("test").master("local").getOrCreate()
  //  session.conf.set("spark.sql.caseSensitive", true) //makes our sql queries case sensitive

  println(s"Session started on Spark version ${session.version}")
  val fPath = "./src/resources/2010-12-01.csv"
  val df = session.read
    .format("csv")
    .option("header", true) //will use first row for header
    .load(fPath)
  println(df.summary().show())
  df.printSchema()
  val df2 = df
    .withColumn("Quantity", col("Quantity").cast("int"))
    .withColumn("UnitPrice", col("UnitPrice").cast("double"))
    .withColumn("CustomerID", col("CustomerID").cast("int"))
//    .withColumn("InvoiceDate", col("InvoiceDate").cast("date"))
    .withColumn("InvoiceDate", col("InvoiceDate").cast("timestamp"))
  df2.printSchema()
  df2.summary().show()
  val negQuantity = df2.filter(col("Quantity") < 0)
  negQuantity.show(10)
  //Filter for prices over 10
  df2.where("UnitPrice > 10").sort(desc("UnitPrice")).show(10)
  df2
    .where("UnitPrice > 5")
    .where("Country != 'United Kingdom'")
    .sort(desc("UnitPrice"))
    .show(10)
  df2.select("Country").distinct().show()
  println(df2.select("Country").distinct().count()) //should be 7
  //TODO select single entries for each country meaning full rows

}
