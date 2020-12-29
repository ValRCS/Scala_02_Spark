import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object ClusteringData extends App {

  //At its core, unsupervised learning is trying to discover patterns or derive a concise representation
  //of the underlying structure of a given dataset.
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

  import org.apache.spark.ml.feature.VectorAssembler
  val va = new VectorAssembler()
    .setInputCols(Array("Quantity", "UnitPrice"))
    .setOutputCol("features")
  val sales = va.transform(spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("./src/resources/retail-data/by-day/*.csv")
    .limit(200)
    .coalesce(1)
    .where("Description IS NOT NULL"))
  sales.cache() //for quicker analysis

  sales.printSchema()
  sales.show(5, false)

  //k-means is one of the most popular clustering algorithms. In this algorithm, a user-specified
  //number of clusters (�) are randomly assigned to different points in the dataset. The unassigned
  //points are then “assigned” to a cluster based on their proximity (measured in Euclidean distance)
  //to the previously assigned point. Once this assignment happens, the center of this cluster (called
  //the centroid) is computed, and the process repeats. All points are assigned to a particular
  //centroid, and a new centroid is computed. We repeat this process for a finite number of iterations
  //or until convergence (i.e., when our centroid locations stop changing). This does not, however,
  //mean that our clusters are always sensical. For instance, a given “logical” cluster of data might
  //be split right down the middle simply because of the starting points of two distinct clusters. Thus,
  //it is often a good idea to perform multiple runs of �-means starting with different initializations.
  //Choosing the right value for � is an extremely important aspect of using this algorithm
  //successfully, as well as a hard task. There’s no real prescription for the number of clusters you
  //need, so you’ll likely have to experiment with different values and consider what you would like
  //the end result to be.

  // https://en.wikipedia.org/wiki/K-means_clustering


  // in Scala
  import org.apache.spark.ml.clustering.KMeans
  val km = new KMeans().setK(5) //so wee need to set the number of clusters before fitting
  println(km.explainParams())
  val kmModel = km.fit(sales)

  // in Scala
  val summary = kmModel.summary
  println(summary.clusterSizes.mkString("Array(", ", ", ")")) // number of points
  println(summary.trainingCost) //so using elbow method you would check when the cost would start flatlining
  //use the elbow as optimal k-means
  //k-means is pretty fast
  //https://en.wikipedia.org/wiki/Elbow_method_(clustering)

  // Make predictions
  val predictions = kmModel.transform(sales)
  predictions.printSchema()
  predictions.show(20,false)


  //there are other clustering modesl
  //some examples here with pluses and minuses for each
  //https://scikit-learn.org/stable/modules/clustering.html


  //TODO model the same data on BisectingKmeans and make predictions!
  import org.apache.spark.ml.clustering.BisectingKMeans
  val bkm = new BisectingKMeans().setK(5).setMaxIter(5)

  val bkmModel = bkm.fit(sales)
  bkmModel.transform(sales).show(20,false)

  //TODO again model and make prediction on our sales!
  import org.apache.spark.ml.clustering.GaussianMixture
  val gmm = new GaussianMixture().setK(5)
  val gmmModel = gmm.fit(sales)
  gmmModel.transform(sales).show(20,false)

  //Latent Dirichlet Allocation (LDA) is a hierarchical clustering model typically used to perform
  //topic modelling on text documents. LDA tries to extract high-level topics from a series of
  //documents and keywords associated with those topics. It then interprets each document as having
  //a variable number of contributions from multiple input topics. There are two implementations
  //that you can use: online LDA and expectation maximization. In general, online LDA will work
  //better when there are more examples, and the expectation maximization optimizer will work
  //better when there is a larger input vocabulary. This method is also capable of scaling to hundreds
  //or thousands of topics.
  //To input our text data into LDA, we’re going to have to convert it into a numeric format. You
  //can use the CountVectorizer to achieve this.

  import org.apache.spark.ml.feature.{Tokenizer, CountVectorizer}
  val tkn = new Tokenizer().setInputCol("Description").setOutputCol("DescOut") //so you do not have to write your own split
  val tokenized = tkn.transform(sales.drop("features"))
  val cv = new CountVectorizer()
    .setInputCol("DescOut")
    .setOutputCol("features")
    .setVocabSize(500)
    .setMinTF(0)
    .setMinDF(0)
    .setBinary(true)
  val cvFitted = cv.fit(tokenized)
  val prepped = cvFitted.transform(tokenized)

  prepped.printSchema()
  prepped.show(20,false)

  // in Scala
  import org.apache.spark.ml.clustering.LDA
  val lda = new LDA().setK(10).setMaxIter(5)
  println(lda.explainParams())
  val model = lda.fit(prepped) //all the modelling work is done here

  // in Scala
  val top3df = model.describeTopics(3)
  top3df.show(false)
  println(cvFitted.vocabulary.size)

  //we can get topic words by hand
  print(cvFitted.vocabulary(0),cvFitted.vocabulary(1),cvFitted.vocabulary(12))

  //TODO make a column in top3df with termIndiced decoded from cvFitted.vocabulary
  //one way would be to make a udf and then create a column

  val termsToWords = udf((numbers: Seq[Int]) => {
    numbers.map(n => cvFitted.vocabulary(n))
  })

  top3df.printSchema()
  val decodedTopics = top3df.
    withColumn("Top Topics", termsToWords(col("termIndices")))

  decodedTopics.printSchema()
  decodedTopics.show(false)

}
