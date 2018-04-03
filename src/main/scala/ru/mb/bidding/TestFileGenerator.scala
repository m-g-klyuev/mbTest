package ru.mb.bidding

import java.io.PrintWriter
import scala.compat.Platform.EOL

object TestFileGenerator {
  def main(args: Array[String]) {
    args.toList match {
      case Nil => throw new Exception("Sorry, no args have been supplied")
      case h::Nil => {
        val orderQuantity = h.toInt
        Some(new PrintWriter("testdata.txt")).foreach{
          p =>
            p.write(genTestData(orderQuantity).map(bid => s"${bid.direction} ${bid.quantity} ${bid.price}${EOL}").reduce(_ + _))
            p.close
        }
      }
      case h::t => throw new Exception("This one expects just one arg")
    }
  }
}
