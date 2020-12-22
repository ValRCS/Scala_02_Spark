import org.apache.spark.sql.SparkSession

object LinRegression extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")
  val df = spark.read.load("./src/resources/regression")
  df.printSchema()
  df.show(10,false)
  df.selectExpr("label").distinct().show()

  import org.apache.spark.ml.regression.LinearRegression
  val lr = new LinearRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
  println(lr.explainParams())
  val lrModel = lr.fit(df)

  val summary = lrModel.summary

  import spark.implicits._ //some implicit magic toDF
  summary.residuals.show()
  println(summary.objectiveHistory.toSeq.toDF.show())
  println(summary.rootMeanSquaredError)
  println(summary.r2)

  val grades =


}
