import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{array_contains, col, explode, expr, map_keys, split, struct}

object ComplexTypes extends App {
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

  val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
  complexDF.createOrReplaceTempView("complexDF")
  complexDF.show(truncate = false)
  complexDF.where(col("complex.InvoiceNo") === 536365).show(false)
  complexDF.where(col("complex.Description").contains("BABUSHKA")).show(false)
  complexDF.where(expr("complex.Description LIKE '%USHK%'")).show(false)

  //you can iterate over rows and if you wish you can convert each row to Sequence of Any types
  df.limit(10).foreach(row => {
    row.toSeq.foreach(print)
    println("")
  }
  )

  //had to import import org.apache.spark.sql.functions.split manually rare of intelliJ not to pick
  df.select(split(col("Description"), " ")).show(5, truncate=false)

  //we can select a single (first in this case) column
  //TODO check what happens to out of bounds
  df.select(split(col("Description"), " ").alias("array_col"))
    .selectExpr("array_col[0]").show(5, truncate=false)

  //so if the array is out of bounds, we just get null in our data
  df.select(split(col("Description"), " ").alias("array_col"))
    .selectExpr("array_col[10]").show(5, truncate=false)

  import org.apache.spark.sql.functions.size
  df.select(size(split(col("Description"), " "))).show(12) // shows 5 and 3

  //we can add these columns to an existing dataframe
  val df2 = df
    .withColumn("array_col", split(col("Description"), " "))
    .withColumn("arr_size", size(col("array_col")))

  df2.show(10,false)

  //here we just generate boolean flags
  df2.select(array_contains(col("array_col"), "METAL")).show(8, false)
  //we can get the rows where there does exist this value in the array_col
  df2.where(array_contains(col("array_col"), "METAL")).show(8, false)

  df.withColumn("splitted", split(col("Description"), " "))
    .withColumn("exploded", explode(col("splitted")))
    .select("Description", "InvoiceNo", "exploded").show(5, false)

  df.withColumn("splitted", split(col("Description"), " "))
    .withColumn("exploded", explode(col("splitted")))
    .show(5, false)

  val dfExploded = df2.withColumn("exploded", explode(col("array_col")))
  dfExploded.show(12,false)
  println(dfExploded.count)
  dfExploded.summary().show(truncate = false)

  df.select(functions.map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(5, false)

  val dfMap = df.withColumn("complex_map", functions.map(col("Description"), col("InvoiceNo")))

  dfMap.selectExpr("complex_map['WHITE METAL LANTERN']").show(5, false)
  println(dfMap.selectExpr("complex_map['WHITE METAL LANTERN']").count)
  dfMap.selectExpr("complex_map['WHITE METAL LANTERN']")
    .na.drop("all", Seq("complex_map[WHITE METAL LANTERN]"))
    .show(5, false)

  dfMap.selectExpr("explode(complex_map)").show(5, false)

  dfMap.show(5, false)
  //we could have multiple keys so the below will not work
  //dfMap.withColumn("key", expr("explode(complex_map)")).show(5, false)

  dfMap.withColumn("Lantern", col("complex_map")
    .getItem("WHITE METAL LANTERN"))
    .show(5,false)

  val keyMap = dfMap.select(map_keys(col("complex_map"))).withColumnRenamed("map_keys(complex_map)", "key_arr")
  println(keyMap.count)
  keyMap.show(5, false)
  keyMap.printSchema()
  val flatKeys = keyMap.selectExpr("key_arr[0]")
    .withColumnRenamed("key_arr[0]", "myKeys")
  flatKeys.show(5, false)
  flatKeys.printSchema()
  val nullKeys = flatKeys.selectExpr("myKeys = null")//FIXME distinct
  nullKeys.show(5, false)
  println(nullKeys.count)
//  val keysDF = dfMap.select(explode(map_keys(col("complex_map")))).distinct()

//  val keysDF = dfMap
//    .na.drop
//    .select(explode(map_keys(col("complex_map")))).distinct()
//  keysDF.show()
//  val keys = keysDF.collect().map(f=>f.get(0))
//  val keyCols = keys.map(f=> col("property").getItem(f).as(f.toString))
//  dfMap.select(col("name") +: keyCols:_*).show(false)




}
