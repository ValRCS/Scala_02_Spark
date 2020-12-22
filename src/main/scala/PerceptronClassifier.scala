import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

object PerceptronClassifier extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")
  // Load and parse the data file, converting it to a DataFrame.
  val data = spark.read.format("libsvm").load("./src/resources/sample_multiclass_classification_data.txt")

  data.printSchema()
  data.show(true)

  // Split the data into train and test
  val Array(train,test) = data.randomSplit(Array(0.6, 0.4), seed = 1234L)
//  val train = splits(0)
//  val test = splits(1)

  // specify layers for the neural network:
  // input layer of size 4 (features), two intermediate of size 5 and 4
  // and output of size 3 (classes) - 3 species of flower at the end
//  val layers = Array[Int](4, 5, 6, 4, 3)
  val layers = Array[Int](4, 5, 4, 3)

  // create the trainer and set its parameters
  val trainer = new MultilayerPerceptronClassifier()
    .setLayers(layers)
    .setBlockSize(128)
    .setSeed(1234L)
    .setMaxIter(100)

  // train the model
  val model = trainer.fit(train)

  // compute accuracy on the test set
  val result = model.transform(test)
  val predictionAndLabels = result.select("prediction", "label")
  val evaluator = new MulticlassClassificationEvaluator()
    .setMetricName("accuracy")

  println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")

  val trainResult = model.transform(train)
  println(s"Train set accuracy = ${evaluator.evaluate(trainResult)}")

}
