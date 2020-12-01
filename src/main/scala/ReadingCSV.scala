import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{asc, desc}

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
    .limit(3000)
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

  //Random sampling
  val seed = 42
  val withReplacement = false //when it is false it will not pick the same row more than 1 time
  val fraction = 0.5 //so 20%
  val dSample = df2.sample(withReplacement, fraction, seed)
  println("Dataframe size", df2.count(), df2.distinct().count())
  //TODO find the duplicate rows in our dataframe DF2

  println(dSample.count())
  println(dSample.distinct().count())
  //Turns out there are duplicate rows already in the original dataframe

  val replSample = df2.sample(true, fraction, seed)
  println(replSample.count())
  println(replSample.distinct().count())

  //Splitting into two dataframes/datasets
  // in Scala
  val dataFrames = df2.randomSplit(Array(0.25, 0.75), seed)
  println(dataFrames(0).count(), dataFrames(1).count()) // False

  val propFrames= df2.randomSplit(Array(5, 4, 3), seed)
  propFrames.foreach(df => println(df.count())) // False

  val uniques = df2.distinct() //dropDuplicates is an alias
  val duplicates = df2.exceptAll(uniques) //.except would have dropped the duplicates
  duplicates.sort("StockCode").show(50)

  println(duplicates.count)
  println(df2.count - df2.distinct.count)

  val fPath2011 = "./src/resources/2011-12-09.csv"
  val df2011 = session.read
    .format("csv")
    .option("header", true) //will use first row for header
    .load(fPath2011)
  df2011.summary().show()
  df2011.printSchema()
  val dfUnion = df.union(df2011) //only works when schema matches
  println(dfUnion.count)
  val fPathUnion = "./src/resources/union2010-2011.csv"
  //careful with saving on a single machine as the CSV could potentially be huge...
//  dfUnion.write.format("csv").mode("overwrite").save(fPathUnion)
  val tmpPath = "./src/resources/tempCSV"
  dfUnion
    .coalesce(1)
    .write.option("header","true")
    .format("csv")
    .mode("overwrite")
    .save(tmpPath)
  val dir = new File(tmpPath)
  dir.listFiles.foreach(println)

//  val newFileRgex = tmpPath + File.separatorChar + ".part-00000.*.csv" //TODO fix regex when needed
  val tmpTsfFile = dir.listFiles.filter(_.toPath.toString.endsWith(".csv"))(0).toString
  (new File(tmpTsfFile)).renameTo(new File(fPathUnion))
//
  dir.listFiles.foreach( f => f.delete ) //delete all the files in the directory
  dir.delete //delete itself
}
