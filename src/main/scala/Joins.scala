import org.apache.spark.sql.SparkSession

object Joins extends App {

  val session = SparkSession.builder().appName("test").master("local").getOrCreate()
  import session.implicits._

  // in Scala
  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")
  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")
  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
    .toDF("id", "status")

  person.show()
  graduateProgram.show()
  sparkStatus.show()

  //create temporary views they are lazily evaluated meaning they will have date when we actually use them
  person.createOrReplaceTempView("person")
  graduateProgram.createOrReplaceTempView("graduateProgram")
  sparkStatus.createOrReplaceTempView("sparkStatus")

  // in Scala
  val joinExpression = person.col("graduate_program") === graduateProgram.col("id")

  person.join(graduateProgram, joinExpression).show()

  //so matches in either side of joinExpression will be added with nulls in places of missing data
  //in a basic outer join
  val outerDf = person.join(graduateProgram, joinExpression, "outer")
  println(outerDf.columns.mkString("Array(", ", ", ")"))

  //TODO join with withColumnRenamed
  //TODO also do joinType = "left_outer"
  //TODO also do joinType = "right_outer"




}
