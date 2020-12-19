import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}

object ClassificationUse extends App {
//  Classification is the task of predicting a label, category, class, or discrete variable given some
//  input features. The key difference from other ML tasks, such as regression, is that the output
//    label has a finite set of possible values (e.g., three classes).

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

  val rawInput = spark.read.format("parquet").load("./src/resources/binary-classification")
  rawInput.printSchema()
  rawInput.show(5, false)
  val bInput = rawInput
    .selectExpr("features", "cast(label as double) as label") //we need double for label for regression tasks
  bInput.show()

  // in Scala
  import org.apache.spark.ml.classification.LogisticRegression
  val lr = new LogisticRegression() //there are many parameters we could pass as seen below
  println(lr.explainParams()) // see all parameters you can adjust to improve your model
  //these parameters will be different for different algorithms
  val lrModel = lr.fit(bInput)

  // in Scala
  println(lrModel.coefficients)
  println(lrModel.intercept)

  val row1 = bInput.selectExpr("features").limit(1)
  row1.printSchema()

  //https://spark.apache.org/docs/1.0.1/mllib-basics.html
  println(lrModel.predict(Vectors.dense(5,3,5)))
  println(lrModel.predict(Vectors.dense(1,13,5)))
  println(lrModel.predict(Vectors.dense(2,-3,-5)))

  //TODO make multiple predictions on a DataFrame column

  //Decision Trees
  //Decision trees are one of the more friendly and interpretable models for performing classification
  //because they’re similar to simple decision models that humans use quite often. For example, if
  //you have to predict whether or not someone will eat ice cream when offered, a good feature
  //might be whether or not that individual likes ice cream. In pseudocode, if
  //person.likes(“ice_cream”), they will eat ice cream; otherwise, they won’t eat ice cream. A
  //decision tree creates this type of structure with all the inputs and follows a set of branches when
  //it comes time to make a prediction. This makes it a great starting point model because it’s easy to
  //reason about, easy to inspect, and makes very few assumptions about the structure of the data. In
  //short, rather than trying to train coeffiecients in order to model a function, it simply creates a big
  //tree of decisions to follow at prediction time. This model also supports multiclass classification
  //and provides outputs as predictions and probabilities in two different columns.
  //While this model is usually a great start, it does come at a cost. It can overfit data extremely
  //quickly. By that we mean that, unrestrained, the decision tree will create a pathway from the start
  //based on every single training example. That means it encodes all of the information in the
  //training set in the model. This is bad because then the model won’t generalize to new data (you
  //will see poor test set prediction performance). However, there are a number of ways to try and
  //rein in the model by limiting its branching structure (e.g., limiting its height) to get good
  //predictive power.

  // in Scala
  import org.apache.spark.ml.classification.DecisionTreeClassifier
  val dt = new DecisionTreeClassifier()
  println(dt.explainParams())
  val dtModel = dt.fit(bInput)

  //https://spark.apache.org/docs/1.0.1/mllib-basics.html
  println(dtModel.predict(Vectors.dense(5,3,5)))
  println(dtModel.predict(Vectors.dense(1,13,5)))
  println(dtModel.predict(Vectors.dense(2,-3,-5)))

  val features = bInput.select("features")
  features.printSchema()
  features.show(10, false)
  //TODO how to make multiple predictions for example for all features in features dataframe
//  val predictions = dtModel.pr


}
