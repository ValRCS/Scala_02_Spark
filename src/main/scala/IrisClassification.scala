import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.CrossValidator
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
  val Array(train, test) = inputData.randomSplit(Array(0.7, 0.3),seed = 2020)
  train.show(5, false)
  test.show(5, false)

  //so we want to train the model on train set
  val lr = new LogisticRegression()
  val lrModel = lr.fit(train)

  val lrPredictions = lrModel.transform(test)
  lrPredictions.printSchema()
  lrPredictions.show(false)

  // obtain evaluator. //so evaluator will check label and prediction and see percentage of accurate answers
  // An
  //evaluator doesn’t help too much when it stands alone; however, when we use it in a pipeline, we
  //can automate a grid search of our various parameters of the models and transformers—trying all
  //combinations of the parameters to see which ones perform the best. Evaluators are most useful in
  //this pipeline and parameter grid context. For classification, there are two evaluators, and they
  //expect two columns: a predicted label from the model and a true label. For binary classification
  //we use the BinaryClassificationEvaluator. This supports optimizing for two different
  //metrics “areaUnderROC” and areaUnderPR.” For multiclass classification, we need to use the
  //MulticlassClassificationEvaluator, which supports optimizing for “f1”,
  //“weightedPrecision”, “weightedRecall”, and “accuracy”.
  //To use evaluators, we build up our pipeline, specify the parameters we would like to test, and
  //then run it and see the results.
  val evaluator = new MulticlassClassificationEvaluator()
    .setMetricName("accuracy")

  // compute the classification error on test data.
  val accuracy = evaluator.evaluate(lrPredictions)
  println(s"Test Error = ${1 - accuracy}, accuracy ${accuracy*100}%")
  //one liner if we do not care about saving the data
  println(s"Train Accuracy (should be 100%): ${evaluator.evaluate(lrModel.transform(train))}")

  //TODO do the above fit and evaluator using DecisionTree
  //use same train and test data sets

  val dt = new DecisionTreeClassifier()
  val dtModel = dt.fit(train)
  val dtPredictions = dtModel.transform(test)
  dtPredictions.show(false)
  println(s"Decision Tree accuracy ${evaluator.evaluate(dtPredictions)*100}%")

  dt.setMaxDepth(1) //setting Decision Tree depth to 1
  val shallowModel = dt.fit(train)
  val shallowPredictions = shallowModel.transform(test)
  shallowPredictions.show(false)
  println(s"Shallow Decision Tree accuracy ${evaluator.evaluate(shallowPredictions)*100}%")

  dt.setMaxDepth(2) //so depth of 2 should do better that is 3 different questions (you always ask 2 questions
  //2nd question will vary depending on the answer to the first question
  println(s"Shallow Decision Tree accuracy ${evaluator.evaluate(dt.fit(train).transform(test))*100}%")

  //so instead of checking each depth by hand we could use the built in Parameter builder
  import org.apache.spark.ml.tuning.ParamGridBuilder
  val params = new ParamGridBuilder()
    .addGrid(dt.maxDepth, Array(1, 2, 3, 4))
    .addGrid(dt.impurity, Array("entropy", "gini")) //so how many columns to use for making decision
    .build()

  val pipeline = new Pipeline()
    .setStages(Array(dt)) //we are not doing any preprocessing we have pretty good data

  val cv = new CrossValidator() //cross Validation will split data into training and test data multiple times across all data
    .setEstimator(pipeline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(params)
    .setNumFolds(2)  // Use 3+ in practice
    .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel

  // Run cross-validation, and choose the best set of parameters.
  val cvModel = cv.fit(inputData) //the slist and evaluation of test will be done for use

  cvModel.avgMetrics.foreach(println)
  val bestModel = cvModel.bestModel //TODO pretty print best hyperparemeters
  val predictions = bestModel.transform(test)
  predictions.show(10, false)

  // in Scala
  import org.apache.spark.ml.classification.RandomForestClassifier
  val rfClassifier = new RandomForestClassifier()
  println(rfClassifier.explainParams())

  val rfPipeline = new Pipeline()
    .setStages(Array(rfClassifier))

  val rfParams = new ParamGridBuilder()
    .addGrid(rfClassifier.numTrees, Array(2, 5, 10, 20))
    .addGrid(rfClassifier.impurity, Array("entropy", "gini")) //so how many columns to use for making decision
    .addGrid(rfClassifier.bootstrap, Array(true, false))
    .build()

  val rcv = new CrossValidator() //cross Validation will split data into training and test data multiple times across all data
    .setEstimator(rfPipeline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(rfParams)
    .setNumFolds(2)  // Use 3+ in practice
    .setParallelism(8)  // Evaluate up to 2 parameter settings in parallel

  val rfModel = rcv.fit(inputData)
  rfModel.avgMetrics.foreach(println)
}
