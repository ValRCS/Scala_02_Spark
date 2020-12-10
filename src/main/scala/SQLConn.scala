object SQLConn extends App {
  println("Testing SQL connection")
  val driver = "org.sqlite.JDBC"
  val path = "C:/sqlite/db/chinook.db"
  val url = s"jdbc:sqlite:/${path}"
  val tableName = "genres"

  import java.sql.DriverManager
  val connection = DriverManager.getConnection(url)
  connection.isClosed()
  connection.close()
}
