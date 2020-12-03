object UsingOption extends App {
  println("Checking Option type")


  def toInt(s: String): Option[Int] = {
    try {
      Some(Integer.parseInt(s.trim))
    } catch {
      case e: Exception => None
    }
  }

  val a = Seq("343252", "3.1415", "3littlepigs", "007", "zero", "9000")
  val v = a.map(toInt)
  v.foreach(println)
  val myInts = v.flatten //one way of getting all the non None values out
  myInts.foreach(println)

  //second way of accessinpes is using getOorElse("some default for None")
  //  v.foreach(el => println(eg our Option tyl.getOrElse("not a number")))

  //third way of accessing Option values with pattern matching
  a.foreach(el => toInt(el) match {
    case Some(i) => println(i)
    case None => println("That didn't work.")
  })
}
