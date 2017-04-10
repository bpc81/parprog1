package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def balanceRec(chars: Array[Char], depth: Int): Boolean = chars.headOption match {
      case None => depth == 0
      case Some('(') => balanceRec(chars.tail, depth+1)
      case Some(')') => if (depth > 0) balanceRec(chars.tail, depth-1) else false
      case Some(_) => balanceRec(chars.tail,depth)
    }
    balanceRec(chars,0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, netDepth: Int, minDepth: Int): (Int, Int) = {
      if (idx == until) (netDepth, minDepth)
      else chars(idx) match {
        case '(' => traverse(idx + 1, until, netDepth + 1, minDepth)
        case ')' => traverse(idx + 1, until, netDepth - 1, minDepth.min(netDepth - 1))
        case _ => traverse(idx + 1, until, netDepth, minDepth)
      }
    }


    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val center = (until + from) / 2
        val ((netLeft, minLeft), (netRight, minRight)) =
          parallel(reduce(from, center), reduce(center, until))
        (netLeft + netRight, minLeft.min(netLeft + minRight))
      }
    }

    reduce(0, chars.length) == (0, 0)

  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
