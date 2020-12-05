import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, expr, first, from_json, get_json_object, json_tuple, kurtosis, last, max, min, regexp_extract, regexp_replace, skewness, stddev, stddev_pop, sum, to_json, var_pop, variance}
import org.apache.spark.sql.types._
//import org.apache.spark.sql.functions

object UsingJSON extends App {
  val session = SparkSession.builder().appName("test").master("local").getOrCreate()
  //  session.conf.set("spark.sql.caseSensitive", true) //makes our sql queries case sensitive

  println(s"Session started on Spark version ${session.version}")
  val fPath = "./src/resources/union2010-2011.csv"
  val jPath = "./src/resources/MOCK_PERSON_DATA.json"

  val df = session.read
    .format("csv")
    .option("inferSchema", "true") //so when this works we do not have to recast the values
    .option("header", true) //will use first row for header
    .load(fPath)
    .withColumn("InvoiceDate", col("InvoiceDate").cast("timestamp"))
  df.printSchema()

  val jsonDF = session.range(1).selectExpr("""
'{"myJSONKey" : {"myJSONValue" : [1, 2, 3, 4, 5]}}' as jsonString""")

  jsonDF.printSchema()
  jsonDF.show(truncate = false)

  jsonDF.select(
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[3]") as "column",
    json_tuple(col("jsonString"), "myJSONKey") as "innerDict").show(2,truncate = false)

  // in Scala

  val parseSchema = new StructType(Array(
    new StructField("InvoiceNo",StringType,true),
    new StructField("Description",StringType,true)))
  val parsedDF = df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")).alias("newJSON"))
    .select(from_json(col("newJSON"), parseSchema), col("newJSON"))
  parsedDF.printSchema()
  parsedDF.show(5, false)

  val jdf = session.read
    .format("json")
    .option("inferSchema", true)
    .option("multiline", true) //this is needed if your json spans multi-line which is usually the case!!
    .load(jPath)
  jdf.printSchema()
  jdf.show(5, false)
//  jdf.createOrReplaceTempView("table_df")
//  val query_latest_rec = """SELECT * FROM table_df ORDER BY id DESC limit 5"""
//  val latest_rec = jdf.selectExpr("SELECT * FROM table_df")
//  latest_rec.show()

  def add50(number:Double):Double = number + 50
  println(add50(2.0))

  import org.apache.spark.sql.functions.udf //udf - user defined function
  val add50udf = udf(add50(_:Double):Double) //so we've registered our function across our network/cluster
  //now let's use it!
  jdf.select(add50udf(col("latitude"))).show(5)

  session.udf.register("add50", add50(_:Double):Double)
  jdf.selectExpr("add50(latitude)").show(2)

  //adjust this function as needed for true plural
  def plural(txt:String):String = if (!txt.endsWith("e") && !txt.endsWith("a")) s"${txt}s" else txt
  //check if SQL already not have plural! maybe make your own prefix for your own functions like vsPlural
  session.udf.register("plural", plural(_:String):String)

  jdf.selectExpr("plural(first_name)").show(5)

  def genderPlural(name:String, gender:String):String = if (gender == "Male") s"${name}s" else name

  session.udf.register("gPlural", genderPlural(_:String, _:String):String)

  jdf.selectExpr("gPlural(first_name, gender)").show(5)
  jdf.withColumn("Plural", expr("gPlural(first_name, gender)"))
    .show(5,false)

  df.select(count("*")).show()
  //There are a number of gotchas when it comes to null values and counting. For instance, when
  //performing a count(*), Spark will count null values (including rows containing all nulls). However,
  //when counting an individual column, Spark will not count the null values.
  df.select(count("Description")).show()

  df.select(countDistinct("StockCode")).show()
  //for large datasets you might want to use aproximate count distint
  df.select(approx_count_distinct("StockCode", 0.1)).show() // 3364

  //so first and last are specific to specific column
  df.select(first("StockCode"), last("StockCode")).show()


  jdf.select(min("latitude"), max("latitude")).show()
  jdf.select(min("savings"), max("savings")).show() //lexigoraphical on strings
  //so "20" < "9" in string sorting, so might need to convert and extract to values

  jdf
    .select(regexp_replace(col("Savings"), "€|,", "").alias("money"))
    .show()
  //to preserver decimal point we have to do 2 replacements
  jdf
    .select(regexp_replace(col("Savings"), "€", "").alias("money"))
    .select(regexp_replace(col("money"), ",", ".").alias("money"))
    .show()

    jdf.withColumn("money", regexp_replace(col("Savings"), "€", ""))
    .show(5, false)

    def mySplit(version:String):String = version.split('.')(0)
    session.udf.register("mySplit", mySplit(_:String):String)

    val mdf = jdf
      .withColumn("money", regexp_replace(col("Savings"), "€", ""))
      .withColumn("money", regexp_replace(col("money"), ",", "."))
      .withColumn("money", col("money").cast("double"))
      .withColumn("majorVersion", regexp_extract(col("app_version"), "(\\d+)", 0))
      .withColumn("majorVersion", col("majorVersion").cast("int"))
      .withColumn("minorVersion", regexp_extract(col("app_version"), "\\d+\\.(\\d+)", 1))
      .withColumn("minorVersion", col("minorVersion").cast("int"))
      .withColumn("version_number", expr("mySplit(app_version)")) //we can use our own function instead of regex or with regex inside
      .withColumn("version_number", col("version_number").cast("int"))
    mdf.show(5, false)
    mdf.printSchema()

    mdf.select(min(col("money"))
      ,count(col("money"))
      ,max(col("money"))
      ,sum(col("money"))
      ,avg(col("money"))
      ,variance(col("money"))
      ,stddev(col("money"))
      ,var_pop(col("money"))
      ,stddev_pop(col("money"))
      ,skewness(col("money"))
      ,kurtosis(col("money"))

    ).show

}