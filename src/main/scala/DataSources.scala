import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}
import com.github.mrpowers.spark.daria.sql.DariaWriters

object DataSources extends App {
  val session = SparkSession.builder().appName("test").master("local").getOrCreate()


  //The foundation for reading data in Spark is the DataFrameReader. We access this through the
  //SparkSession via the read attribute:
  //spark.read
  //After we have a DataFrame reader, we specify several values:
  //The format
  //The schema
  //The read mode
  //A series of options
//  val dfReader = session.read //we are going to usually use session.read which instances a
//  println(s"Session started on Spark version ${session.version}")
//  val fPath = "./src/resources/union2010-2011.csv"
//  val df = session.read
//    .format("csv")
//    .option("inferSchema", "true") //so when this works we do not have to recast the values
//    .option("header", true) //will use first row for header
//    .load(fPath)
//    .withColumn("InvoiceDate", col("InvoiceDate").cast("timestamp"))
//    .withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
//  df.printSchema()
//  df.show(5, false)

  val pPATH = "./src/resources/union.parquet/part-00000-04255bb1-f7c9-4032-b901-ef9bb47c6b8b-c000.snappy.parquet"
  val df = session.read.format("parquet").load(pPATH)
  df.show(7,false)
  df.printSchema()


  //WARNING
  //Even though there are only two options, you can still encounter problems if you’re working with
  //incompatible Parquet files. Be careful when you write out Parquet files with different versions of
  //Spark (especially older ones) because this can cause significant headache.
//  df
//    .write
//    .format("parquet")
//    .mode("overwrite")
//    .save("./src/resources/union.parquet")

  //so if we want to avoid those crazy long file names we can use this spark-daria library
  //https://mungingdata.com/apache-spark/output-one-file-csv-parquet/
  DariaWriters.writeSingleFile(
    df = df,
    format = "csv",
    sc = session.sparkContext,
    tmpFolder = "./src/resources/tmp", //this looks the place where the files are stored before deleting
    filename = "./src/resources/singleCSV.csv"
  )

}
