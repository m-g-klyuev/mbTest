package ru.mb

import scala.annotation.tailrec
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

package object bidding {

  val EXECUTOR_POOL_SIZE = 4

  val SPLIT_FLAG = 2000

  val MAX_BID_PRICE = 100

  val MAX_ORDER_QUANTITY = 1000

  implicit class Pipe[A](val v: A) extends AnyVal {
    def |>[B](f: A => B): B = f(v)
  }

  def genTestData(entryNum: Int): List[Bid] = {
    @tailrec
    def gen(i: Int, acc: List[Bid]): List[Bid] = {
      if (i <= entryNum) {
        val bid = Bid(
          Random.nextBoolean() match {
            case true => Bid.S
            case _ => Bid.B
          }
          , Random.nextInt(MAX_ORDER_QUANTITY - 1) + 1
          , BigDecimal.valueOf(Random.nextDouble() * MAX_BID_PRICE).setScale(2, RoundingMode.HALF_UP).toDouble
        )
        gen(i+1, bid :: acc)
      }
      else acc
    }
    gen(1, Nil)
  }

  case class Bid(direction: Char, quantity: Int, price: Double)

  case class PriceByOrderNum(price: Double, quantity: Int) {
    val totalVolume = price * quantity
  }

  case class OptSalePricesByVolume(optPrices: List[PriceByOrderNum], totalVolume: Double)

  case class ResultVolumeByOrder(reSum: Double, sTotVolume: Double, sOrderAmount: Int)

  object Bid {
    val B = 'B'
    val S = 'S'

    def buyOrderProt = this (B, 100, 50.05)

    def selOrderProt = this (S, 100, 49.95)
  }

}
