import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, expr}

object SQLConn extends App {
  println("Testing SQL connection")
  val driver = "org.sqlite.JDBC"
  val path = "C:/sqlite/db/chinook.db"
  val url = s"jdbc:sqlite:/${path}"
  val tableName = "genres"

  //good for troubleshooting connections
//  import java.sql.DriverManager
//  val connection = DriverManager.getConnection(url)
//  connection.isClosed()
//  connection.close()

  val session = SparkSession.builder().appName("test").master("local").getOrCreate()


  // in Scala
  val df = session.read.format("jdbc").option("url", url)
    .option("dbtable", tableName).option("driver", driver).load()

  df.printSchema()
  df.show(5, false)

  val artists =  session.read.format("jdbc").option("url", url)
    .option("dbtable", "artists").option("driver", driver).load()
  artists.printSchema()
  artists.sample(0.01).show(25,false)
  artists.sort(desc("Name")).show(25)
  artists.sort(expr("Name desc")).show(15, false)


  val tracks = session.read.format("jdbc").option("url", url)
    .option("dbtable", "tracks").option("driver", driver).load()
  tracks.printSchema()
}
