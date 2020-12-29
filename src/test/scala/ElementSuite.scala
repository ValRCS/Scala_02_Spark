import com.github.valrcs.spark.MyUtil
import org.scalatest._
import flatspec._
import matchers._

class ElementSuite extends AnyFlatSpec with should.Matchers {

  "My Calc" should "Calculate values 2.5 times the input" in {
    val res = MyUtil.calcValue(10)
    res should be (25)
    val res2 = MyUtil.calcValue(2)
    res2 should be (5)
//    val stack = new Stack[Int]
//    stack.push(1)
//    stack.push(2)
//    stack.pop() should be (2)
//    stack.pop() should be (1)
  }

  "Extreme Calc" should "get 10 on 0" in {
    val res = MyUtil.calcValue(0)
    res should be (10)
    MyUtil.calcValue(30) should be (75)
  }

  "Circle Area" should "follow standard PI formula" in {
    val res = MyUtil.calcCircleArea(5)
    res should be (5*5*math.Pi)
    val res2 = MyUtil.calcCircleArea(10)
    res2 should be (10*10*math.Pi)
    val res3 = MyUtil.calcCircleArea(25)
    res3 should be (25*25*math.Pi)
    MyUtil.calcCircleArea(0) should be (0) //always good idea to test 0
    //also good candidates to test would 1 , also -1 (we'd have to decide what -1 gives of course
  }

}
