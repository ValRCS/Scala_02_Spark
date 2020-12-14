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
}
