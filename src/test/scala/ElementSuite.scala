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
  }
}
