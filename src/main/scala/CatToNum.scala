import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SparkSession

object CatToNum extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")
  var simpleDF = spark.read.json("./src/resources/simple-ml")
  simpleDF.printSchema()
  simpleDF.show(5, truncate = false)

  //The simplest way to index is via the StringIndexer, which maps strings to different numerical
  //IDs. Spark’s StringIndexer also creates metadata attached to the DataFrame that specify what
  //inputs correspond to what outputs. This allows us later to get inputs back from their respective
  //index values:
  val lblIndexer = new StringIndexer().setInputCol("lab").setOutputCol("labelInd")
  val idxRes = lblIndexer.fit(simpleDF).transform(simpleDF)
  idxRes.show()
  idxRes.write.format("parquet").mode("overwrite")
    .save("./src/resources/parq-cat")

  //We can also apply StringIndexer to columns that are not strings, in which case, they will be
  //converted to strings before being indexed:
  //// in Scala
  val valIndexer = new StringIndexer()
  .setInputCol("value1")
  .setOutputCol("valueInd")
  valIndexer.fit(simpleDF).transform(simpleDF).show()

  //When inspecting your machine learning results, you’re likely going to want to map back to the
  //original values. Since MLlib classification models make predictions using the indexed values,
  //this conversion is useful for converting model predictions (indices) back to the original
  //categories. We can do this with IndexToString. You’ll notice that we do not have to input our
  //value to the String key; Spark’s MLlib maintains this metadata for you. You can optionally
  //specify the outputs
  // in Scala
  import org.apache.spark.ml.feature.IndexToString
  val labelReverse = new IndexToString().setInputCol("labelInd")
  labelReverse.transform(idxRes).show(truncate=false)

  val readDF = spark.read.format("parquet")
    .load("./src/resources/parq-cat")

  readDF.printSchema()
  readDF.show(truncate=false)

  //so at least with parquet we preserve the metadata and we can get back the original names
  labelReverse.transform(readDF).show(truncate=false)

  //one hot encoding creates a set of flags out of all the possible choice, so no value is more important than other
  val lblIndxr = new StringIndexer().setInputCol("color").setOutputCol("colorInd")
  val colorLab = lblIndxr.fit(simpleDF).transform(simpleDF.select("color"))
  val ohe = new OneHotEncoder().setInputCol("colorInd")
//  ohe.transform(colorLab).show()
  colorLab.show(truncate = false)
//TODO check the new syntax for the One Hot Encoder
}
