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
  //TODO why Name desc did not consider desc
  artists.sort(expr("Name desc")).show(15, false)


  val tracks = session.read.format("jdbc").option("url", url)
    .option("dbtable", "tracks").option("driver", driver).load()
  tracks.printSchema()

  // in Scala
  tracks.filter("Composer in ('AC/DC', 'Apocalyptica')").show(25, false)

  //SQL query pushed down to SQL DB engine
  // in Scala
  val pushdownQuery = """(SELECT * FROM albums a
  JOIN artists a2
  ON a.ArtistId = a2.ArtistId)""" // even without alias you do need parenthesis
//  val pushdownQuery = """(SELECT * FROM albums) AS myAlbums"""
  val albumsWithArtists = session
    .read.format("jdbc")
    .option("url", url)
    .option("dbtable", pushdownQuery)
    .option("driver", driver)
    .load()
  albumsWithArtists.printSchema()
  albumsWithArtists.show(15,false)

  //TODO create tracksDF with album joined with artist as well
  //TODO filter only the long songs over 10 minutes long
  //TODO you can do this with pure SQL or can do itwith mixture of regular SQL and Spark SQL
}