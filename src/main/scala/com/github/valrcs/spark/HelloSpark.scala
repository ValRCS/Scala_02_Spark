package com.github.valrcs.spark

import org.apache.spark.sql.SparkSession

object HelloSpark extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")
  val newRow = MyRow()
  println(newRow)
  println(MyUtil.calcValue(55))
}
