import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.sql.SparkSession

object PCA extends App {
//  PCA
//  Principal Components Analysis (PCA) is a mathematical technique for finding the most
//  important aspects of our data (the principal components). It changes the feature representation of
//    our data by creating a new set of features (“aspects”). Each new feature is a combination of the
//    original features. The power of PCA is that it can create a smaller set of more meaningful
//  features to be input into your model, at the potential cost of interpretability.
//  You’d want to use PCA if you have a large input dataset and want to reduce the total number of
//    features you have. This frequently comes up in text analysis where the entire feature space is
//  massive and many of the features are largely irrelevant. Using PCA, we can find the most
//  important combinations of features and only include those in our machine learning model. PCA
//  takes a parameter �, specifying the number of output features to create. Generally, this should be
//  much smaller than your input vectors’ dimension.
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

  val scaleDF = spark.read.parquet("./src/resources/simple-ml-scaling")
  scaleDF.printSchema()
  scaleDF.show(false)

  // in Scala
  import org.apache.spark.ml.feature.PCA
  val pca = new PCA().setInputCol("features").setK(2)
  pca.fit(scaleDF).transform(scaleDF).show(false)

  pca
    .setK(1)
    .setOutputCol("singleFeature")
    .fit(scaleDF) //so before transformation the pca has to analyze the data
    .transform(scaleDF)
    .show(false)

  pca
    .setK(3)
    .setOutputCol("tripleFeature")
    .fit(scaleDF) //so before transformation the pca has to analyze the data
    .transform(scaleDF)
    .show(false)

//  pca
//    .setK(4) //going against the goal so not for production, we can increase the dimensions..
//    .setOutputCol("tripleFeature")
//    .fit(scaleDF) //so before transformation the pca has to analyze the data
//    .transform(scaleDF)
//    .show(false)

  //alternative or supplement to PCA
  //ChiSqSelector
  //ChiSqSelector leverages a statistical test to identify features that are not independent from the
  //label we are trying to predict, and drop the uncorrelated features. It’s often used with categorical
  //data in order to reduce the number of features you will input into your model, as well as to
  //reduce the dimensionality of text data (in the form of frequencies or counts). Since this method is
  //based on the Chi-Square test, there are several different ways we can pick the “best” features.
  //The methods are numTopFeatures, which is ordered by p-value; percentile, which takes a
  //proportion of the input features (instead of just the top N features); and fpr, which sets a cut off
  //p-value.
  // in Scala
  import org.apache.spark.ml.feature.{ChiSqSelector, Tokenizer}
  val sales = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("./src/resources/retail-data/by-day/*.csv") //we took multiple csv files and loaded them together into a single DF
    .coalesce(5)
    .where("Description IS NOT NULL")
    .limit(5000) //else it would take too long to analyze
  val tkn = new Tokenizer().setInputCol("Description").setOutputCol("DescOut")
  val tokenized = tkn
    .transform(sales.select("Description", "CustomerId"))
    .where("CustomerId IS NOT NULL")
  tokenized.show(false)
  val cv = new CountVectorizer()
    .setInputCol("DescOut")
    .setOutputCol("countVec")
    .setVocabSize(500)
    .setMinTF(1) //the word should at least once
    .setMinDF(2) //the word should appear in at least two rows
  val fittedCV = cv.fit(tokenized)
//  fittedCV.show(false)
  val prechi = fittedCV.transform(tokenized)
  val chisq = new ChiSqSelector()
    .setFeaturesCol("countVec")
    .setLabelCol("CustomerId") //in this is prediction target which we will use to cut down number of features
    .setNumTopFeatures(5)
  chisq
    .fit(prechi)
    .transform(prechi)
//    .drop("customerId", "Description", "DescOut")
    .show(false)
}
