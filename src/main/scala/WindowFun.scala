import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, desc, max, mean, min, rank, to_date}

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

  val windowSpec = Window
    .partitionBy("CustomerId", "date")
    .orderBy(col("Quantity").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)


  val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
  val meanPurchaseQuantity = mean(col("Quantity")).over(windowSpec)
  val minPurchaseQuantity = min(col("Quantity")).over(windowSpec)
  //TODO explore why min is giving the current
  val purchaseDenseRank = dense_rank().over(windowSpec)
  val purchaseRank = rank().over(windowSpec)

  df.where("CustomerId IS NOT NULL").orderBy("CustomerId")
    .select(
      col("CustomerId"),
      col("InvoiceNo"),
      col("Description"),
      col("date"),
      col("Quantity"),
      purchaseRank.alias("quantityRank"),
      purchaseDenseRank.alias("quantityDenseRank"),
      minPurchaseQuantity.alias("minPurchaseQuantity"),
      meanPurchaseQuantity.alias("meanPurchaseQuantity"),
      maxPurchaseQuantity.alias("maxPurchaseQuantity"))
    .show(50, false)

  // in Scala
  val dfNoNull = df.drop()
  dfNoNull.createOrReplaceTempView("dfNoNull")
  dfNoNull
    .groupBy("customerId", "stockCode")
    .agg("Quantity"->"sum"
    )
    .orderBy(desc("customerId"), desc("stockCode"))
    .show(5,false)


}
