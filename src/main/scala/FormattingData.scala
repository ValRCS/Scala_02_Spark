import org.apache.spark.sql.SparkSession

object FormattingData extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

  val sales = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("./src/resources/retail-data/by-day/*.csv") //we took multiple csv files and loaded them together into a single DF
    .coalesce(5)
    .where("Description IS NOT NULL")
//  val fakeIntDF = spark.read.parquet("./src/resources/simple-ml-integers")
  val fakeIntDF = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("./src/resources/my-ints.csv") //we took multiple csv files and loaded them together into a single DF
  var simpleDF = spark.read.json("./src/resources/simple-ml")
  val scaleDF = spark.read.parquet("./src/resources/simple-ml-scaling")
  fakeIntDF.printSchema()
  fakeIntDF.show(truncate = false)

  sales.cache()
  sales.show(5, truncate= false)

  //The Tokenizer is an example of a transformer. It tokenizes a string, splitting on a given
  //character, and has nothing to learn from our data; it simply applies a function
  import org.apache.spark.ml.feature.Tokenizer
  val tkn = new Tokenizer().setInputCol("Description")
  tkn.transform(sales.select("Description")).show(5, false)

  //StandardScaler, which scales your input column
  //according to the range of values in that column to have a zero mean and a variance of 1 in each
  //dimension. For that reason it must first perform a pass over the data to create the transformer.
  // in Scala
  scaleDF.printSchema()
  scaleDF.show(5, truncate = false)
  import org.apache.spark.ml.feature.StandardScaler
  val ss = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaled_features")
  ss.fit(scaleDF).transform(scaleDF).show(false)



  import org.apache.spark.ml.feature.RFormula
  val supervised = new RFormula()
    .setFormula("lab ~ . + color:value1 + color:value2")
  supervised.fit(simpleDF).transform(simpleDF).show(truncate = false)

  val intFormula = new RFormula()
    .setFormula("int1 ~ . + int2 + int3 + int2:int3") //so : means multiply
  val intFeatureLab = intFormula.fit(fakeIntDF).transform(fakeIntDF)

    val iScaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("features_scaled")
  iScaler.fit(intFeatureLab).transform(intFeatureLab).show(truncate = false)

}
