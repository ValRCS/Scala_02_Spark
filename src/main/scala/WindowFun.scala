import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, desc, expr, max, mean, min, rank, sum, to_date}

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
  val dfNoNull = df.na.drop()
  dfNoNull.createOrReplaceTempView("dfNoNull")
  dfNoNull
    .groupBy("customerId", "stockCode")
    .agg("Quantity"->"sum"
    )
    .orderBy(desc("customerId"), desc("stockCode"))
    .show(5,false)

  //so grouping sets is not available through API in any language

  //now onto rollups
  val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
    .orderBy("total_quantity", "Date")
  rolledUpDF.show()

  //so null in rollup means everything
//  dfNoNull.where(expr("Country = null")).show(10,false)
  //so this is country totals
  rolledUpDF.where("Date IS NULL").show(10, truncate = false)
  rolledUpDF.where("Country IS NULL").show(10, truncate = false)
  dfNoNull.groupBy("Country").agg(sum("Quantity")).show(10, truncate = false)

  //A cube takes the rollup to a level deeper. Rather than treating elements hierarchically, a cube
  //does the same thing across all dimensions. This means that it won’t just go by date over the
  //entire time period, but also the country.
  dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))
    .select("Date", "Country", "sum(Quantity)").orderBy("Date").show(15, false)

  dfNoNull.cube("Date", "Country","InvoiceNo").agg(sum(col("Quantity")))
    .select("Date", "Country","InvoiceNo", "sum(Quantity)").orderBy(desc("sum(Quantity)"))
    .show(25, false)

  // in Scala
  val pivoted = df.groupBy("date").pivot("Country").sum()

  pivoted.show(25,false)

  //big pivot with performing aggregration on each value from the country column
  df.groupBy("date")
    .pivot("Country")
    .agg("Quantity"->"sum", "Quantity"->"max","Quantity"->"mean", "Quantity"->"min")
    .show(10, false)




}
