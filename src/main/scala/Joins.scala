import org.apache.spark.sql.SparkSession

object Joins extends App {

  val session = SparkSession.builder().appName("test").master("local").getOrCreate()
  import session.implicits._

  // in Scala
  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)),
    (3, "Valdis", 42, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")
  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (3, "Bachelors", "Dept of Basket Weaving", "UC Berkeley"),
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
  //join with withColumnRenamed
  val outerDf = person
    .withColumnRenamed("id","personId")
    .join(graduateProgram, joinExpression, "outer")

  outerDf.show()
  println(outerDf.columns.mkString("Array(", ", ", ")"))

  //Left outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from
  //the left DataFrame as well as any rows in the right DataFrame that have a match in the left
  //DataFrame. If there is no equivalent row in the right DataFrame, Spark will insert null
  person
    .withColumnRenamed("id","personId")
    .join(graduateProgram, joinExpression, "left_outer")
    .show()

  //Right outer joins evaluate the keys in both of the DataFrames or tables and includes all rows
  //from the right DataFrame as well as any rows in the left DataFrame that have a match in the right
  //DataFrame. If there is no equivalent row in the left DataFrame, Spark will insert null
  person
    .withColumnRenamed("id","personId")
    .join(graduateProgram, joinExpression, "right_outer")
    .show()

//  Semi joins are a bit of a departure from the other joins. They do not actually include any values
//  from the right DataFrame. They only compare values to see if the value exists in the second
//    DataFrame. If the value does exist, those rows will be kept in the result, even if there are
//    duplicate keys in the left DataFrame. Think of left semi joins as filters on a DataFrame, as
//  opposed to the function of a conventional join:
  //so we find only grad programs which had any people in them at some point
  graduateProgram.join(person, joinExpression, "left_semi").show()

  //Left anti joins are the opposite of left semi joins. Like left semi joins, they do not actually
  //include any values from the right DataFrame. They only compare values to see if the value exists
  //in the second DataFrame. However, rather than keeping the values that exist in the second
  //DataFrame, they keep only the values that do not have a corresponding key in the second
  //DataFrame. Think of anti joins as a NOT IN SQL-style filter:
  graduateProgram.join(person, joinExpression, "left_anti").show()

  //The last of our joins are cross-joins or cartesian products. Cross-joins in simplest terms are inner
  //joins that do not specify a predicate. Cross joins will join every single row in the left DataFrame
  //to ever single row in the right DataFrame. This will cause an absolute explosion in the number of
  //rows contained in the resulting DataFrame. If you have 1,000 rows in each DataFrame, the cross-
  //join of these will result in 1,000,000 (1,000 x 1,000) rows.
  //so this will not work as a cross join
  graduateProgram.join(person, joinExpression, "cross").show()
  //this will do crossjoin since we need no join expression for cross join
  graduateProgram.crossJoin(person).show() //so 4 programs x 4 persons 16 rows

//regular inner join
  person
    .withColumnRenamed("id","personId")
    .join(graduateProgram, joinExpression)
    .show()

  //so if we rename columns used in join we
  //then we have to do renaming and join expression ahead of time
  val gradProgUpdated = graduateProgram
    .withColumnRenamed("department","dept.")
    .withColumnRenamed("id","gradId")

  person
    .withColumnRenamed("id","personId")
    .join(gradProgUpdated
      ,person.col("graduate_program") === gradProgUpdated.col("gradId"))
    .show()
}
