import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, column, expr, lit}

object RowTransformations extends App {
  println("Row transformations")
  val session = SparkSession.builder().appName("test").master("local").getOrCreate()
//  session.conf.set("spark.sql.caseSensitive", true) //makes our sql queries case sensitive

  println(s"Session started on Spark version ${session.version}")
  val fPath = "./src/resources/2015-summary.json" //json is not quite right but still works
  val df = session.read.format("json")
    .load(fPath)
  df.createOrReplaceTempView("dfTable")
  println(df.select("DEST_COUNTRY_NAME").show(12))
  println(df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(12))

  println(df.select(expr("DEST_COUNTRY_NAME as destination")).show(4))
  println(df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(4))
  val df2 = df.selectExpr(
    "*", // include all original columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
  println(df2.show())
  //so we can save results of selectExpr as another dataFrame
  println(df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show())

  //so we are creating a new dataframe with new column with ones
  println(df.select(expr("*"), lit(1).as("ONE")).show(10))
  //TODO see if this fixable to give above result println(df.selectExpr("*, 1 AS One").show(4))
  println(df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
    .show(2))

  //testing case insensitivity settings
  val dfDropped = df2.drop("Dest_country_name", "origin_country_name")
  println(dfDropped.show(5))

}
