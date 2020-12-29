package com.github.valrcs.spark

import org.apache.spark.sql.SparkSession

object StreamingUse extends App {
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

  // in Scala
  val static = spark.read.json("./src/resources/activity-data/")
  val dataSchema = static.schema
//  print(dataSchema)
  static.printSchema()
  static.show(10,false)


  //
  // in Scala
  val streaming = spark.readStream.schema(dataSchema) //so this is a shortcut to making our own schema by hand in production
    .option("maxFilesPerTrigger", 1).json("./src/resources/activity-data/") //in production we'd not limit to one file per trigger

  // in Scala
  val activityCounts = streaming.groupBy("gt").count()

//  Because this code is being written in local mode on a small machine, we are going to set the
//    shuffle partitions to a small value to avoid creating too many shuffle partitions:
  spark.conf.set("spark.sql.shuffle.partitions", 5)
  //activityCounts.show(false) //not as simple as that


  // in Scala Now that we set up our transformation, we need only to specify our action to start the query. As
  //mentioned previously in the chapter, we will specify an output destination, or output sink for our
  //result of this query. For this basic example, we are going to write to a memory sink which keeps
  //an in-memory table of the results.
  //In the process of specifying this sink, we’re going to need to define how Spark will output that
  //data. In this example, we use the complete output mode. This mode rewrites all of the keys along
  //with their counts after every trigger:

  val activityQuery = activityCounts.writeStream.queryName("activity_counts")
    .format("memory").outputMode("complete")
    .start()



  println("Testing Queries")
  // in Scala
  for( i <- 1 to 10 ) {
    spark.sql("SELECT * FROM activity_counts").show()
    Thread.sleep(2000)
  }


  //When we run the preceding code, we also want to include the following line:

  //After this code is executed, the streaming computation will have started in the background. The
  //query object is a handle to that active streaming query, and we must specify that we would like
  //to wait for the termination of the query using activityQuery.awaitTermination() to prevent
  //the driver process from exiting while the query is active.
  // in Scala
  activityQuery.awaitTermination()
}
