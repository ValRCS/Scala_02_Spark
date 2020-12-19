import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

object IrisClassification extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")
  // load data file.
  val inputData = spark.read.format("libsvm")
    .load("./src/resources/iris.scale.txt")

  inputData.printSchema()
  inputData.show(5, false)

  // generate the train/test split.,IMPORTANT! do not use test data to fit the model!
  val Array(train, test) = inputData.randomSplit(Array(0.8, 0.2))
  train.show(5, false)
  test.show(5, false)

  //so we want to train the model on train set
  val lr = new LogisticRegression()
  val lrModel = lr.fit(train)

  val lrPredictions = lrModel.transform(test)
  lrPredictions.printSchema()
  lrPredictions.show(false)

  // obtain evaluator. //so evaluator will check label and prediction and see percentage of accurate answers
  val evaluator = new MulticlassClassificationEvaluator()
    .setMetricName("accuracy")

  // compute the classification error on test data.
  val accuracy = evaluator.evaluate(lrPredictions)
  println(s"Test Error = ${1 - accuracy}, accuracy ${accuracy*100}%")
  //one liner if we do not care about saving the data
  println(s"Train Accuracy (should be 100%): ${evaluator.evaluate(lrModel.transform(train))}")
}
