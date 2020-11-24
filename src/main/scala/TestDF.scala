object TestDF extends App {
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.types._ //so we get access to these types
  import org.apache.spark.sql.{types => SparkTypes}

  val session = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${session.version}")

  // in Scala
  val df = session.range(500).toDF("number")
  val df2 = df.select(df.col("number") + 10) //so loop for free
  println(df.summary().show())
  println(df2.summary().show())

  val df3 = session.range(6).toDF().collect()
  df3.foreach(println)

  val b = ByteType //so ByteType comes from

  //this is schema on read, meaning I let the data types be determined automatically
  //it works pretty well for most part
  //in production you might want to explicitly define data types

  val fPath = "C:\\Users\\val-wd\\Github\\Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json"
  val flightDF = session.read.format("json")
    .load(fPath)
  println(flightDF.summary().show())
  //recipe to get data types
  val types = flightDF.schema.fields.map(f => f.dataType)
  types.foreach(println)
  println(flightDF.schema)

  //manual datatype schema example
  val myManualSchema = StructType(Array(
    StructField("DEST_COUNTRY_NAME", StringType, true),
    StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    StructField("count", SparkTypes.IntegerType, false, //TODO check what false flag does
      Metadata.fromJson("{\"hello\":\"world\"}"))
  ))
  val df4 = session.read.format("json").schema(myManualSchema)
    .load(fPath)
  df4.schema.fields.foreach(f => println(f.dataType))




}
