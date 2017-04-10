package reductions

import scala.annotation._
import org.scalameter._
import common._

import scala.util.{Try, Failure, Success}

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
    def balanceRec(idx: Int, depth: Int): Boolean = Try(chars(idx)) match {
      case Success('(') => balanceRec(idx+1, depth+1)
      case Success(')') => if (depth > 0) balanceRec(idx+1, depth-1) else false
      case Success(_) => balanceRec(idx+1, depth)
      case Failure(_) => depth == 0
    }

    balanceRec(0,0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, netDepth: Int, minDepth: Int): (Int, Int) = {
//      if (idx == until) (netDepth, minDepth)
      if (idx < until) chars(idx) match {
        case '(' => traverse(idx + 1, until, netDepth + 1, minDepth)
        case ')' => traverse(idx + 1, until, netDepth - 1, minDepth.min(netDepth - 1))
        case _ => traverse(idx + 1, until, netDepth, minDepth)
      }
      else (netDepth, minDepth)
    }


    def reduce(from: Int, until: Int): (Int, Int) = {
//      if (until - from <= threshold) traverse(from, until, 0, 0)
      if (until - from > threshold) {
        val center = (until + from) / 2
        val ((netLeft, minLeft), (netRight, minRight)) =
          parallel(reduce(from, center), reduce(center, until))
        (netLeft + netRight, minLeft.min(netLeft + minRight))
      }
      else traverse(from, until, 0, 0)
    }

    reduce(0, chars.length) == (0, 0)

  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
