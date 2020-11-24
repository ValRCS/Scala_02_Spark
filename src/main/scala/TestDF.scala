import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper

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

//  val fPath = "C:\\Users\\val-wd\\Github\\Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json"
  val fPath = "./src/resources/2015-summary.json" //json is not quite right but still works
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

  import org.apache.spark.sql.functions.{col, column}
  val myCol = col("someColumnName")
  val myCol2 = column("someColumnName")
  val myCol3 = $"MyColumn"
  val myCol4 = 'myColumn //for those who are really the shortest way

  val df5 = df.select((df.col("number") + 10) * 100) //so any type of expression inside select
  println(df5.summary().show())

  //so we can rewrite the above using selectExpr which will lead to more SQL like code
  val df6 = df.selectExpr("(number + 55) * 20 ")
  println(df6.summary().show())

  //we can get the actual column names
  val cols = session.read.format("json").load(fPath)
    .columns
  cols.foreach(println)

  //so first row
  println(df4.first())

  // in Scala
  import org.apache.spark.sql.Row
  val myRow = Row("Hello", null, 1, false)
  println(myRow.schema)
  println(myRow)

  val destCountry = df4.first()(0).asInstanceOf[String]
  println(destCountry)
  val originCountry = df4.first()(1).asInstanceOf[String]
  println(originCountry)
  val numberOfFlights = df4.first()(2).asInstanceOf[Int]
  println(s"Flights Flown from $originCountry to $destCountry: $numberOfFlights")


}
