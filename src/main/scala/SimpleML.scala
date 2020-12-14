import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession

object SimpleML extends App {
  val session = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${session.version}")

  // in Scala
  var df = session.read.json("./src/resources/simple-ml")
  df.orderBy("value2").show()

  val supervised = new RFormula()
      .setFormula("lab ~ . + color:value1 + color:value2")

  //apply formula
  val fittedRF = supervised.fit(df)

  val preparedDF = fittedRF.transform(df)
  preparedDF.show()

  //in supervised learning we want to train on one data set and test on completely separate data set
  val Array(train, test) = preparedDF.randomSplit(Array(0.7, 0.3)) //so 70% for training and 30% for testing

  train.describe().show()

  test.describe().show()

  //how to save on Parquet
//  preparedDF
//    .write
//    .format("parquet")
//    .mode("overwrite")
//    .save("./src/resources/simple-ml.parquet")
//
//  val newPath = "./src/resources/simple-ml.parquet"
//  val newDF = session.read
//    .format("parquet")
////    .option("inferSchema", "true") // for parquet all the data types are encoded
////    .option("header", true)
//    .load(newPath)
//  newDF.printSchema()
//  newDF.show(truncate = false)
}
