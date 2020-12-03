import org.apache.spark.sql.functions.{coalesce, sum}

object NullOrderings extends App {
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions.{col, current_date, current_timestamp, desc, expr, regexp_extract, to_timestamp}

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

  df.select(coalesce(col("Description"), col("CustomerId"))).show(truncate = false)
  println(df.count)
  //so this recipe will drop any rows which contain any cells with missing values
  println(df.na.drop.count)
  //so this recipe will drop rows where specific columns have misssing values
  println(df.na.drop("all", Seq("StockCode", "UnitPrice")).count)
  println(df.na.drop("all", Seq("Country")).count)
  val columns = df.columns
  df.columns.foreach(col => {
    println(s"Checking column: $col")
    val clean_count = df.na.drop("all", Seq(col)).count
    println(df.count, clean_count, df.count-clean_count)
  })
  //alternative on how to count missing values
  //https://stackoverflow.com/questions/44413132/count-the-number-of-missing-values-in-a-dataframe-spark
  df.select(df.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show

  //fill all null values in any columns of type String
  val df2 = df.na.fill("Replacement Stringy")
  df2.where(col("Description") === "Replacement Stringy").show
  //TODO how to filter any column for this stringy, so filter all columns for this
  //df2.where()

  val df3 = df.na.fill(9000, Seq("CustomerId"))
  println(df3.where(expr("CustomerId = 9000")).count)

//  val df4 = df.na.replace("Description", Map("" -> "UNKNOWN")) // it is not picking up empty strings there re none
////  df4.where(col("Description").contains("UNKNOWN")).show
//  df4.where(col("Description") === "UNKNOWN").show

  val df4 = df.na.replace("UnitPrice",Map(0.0 -> 9000.5))
  df4.where(expr("UnitPrice = 9000.5")).show
}
