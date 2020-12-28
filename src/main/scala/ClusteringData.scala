import org.apache.spark.sql.SparkSession

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
    .limit(50)
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

  //TODO again model and make prediction on our sales!
  import org.apache.spark.ml.clustering.GaussianMixture
  val gmm = new GaussianMixture().setK(5)



}
