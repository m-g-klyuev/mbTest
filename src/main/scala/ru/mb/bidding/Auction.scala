package ru.mb.bidding

import scala.compat.Platform.EOL
import java.util.concurrent.{Callable, ExecutorService, Executors}
import scala.io.Source
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object Auction {

  def main(args: Array[String]) {

    Try(
      args.toList match {
        case Nil => throw new Exception("Sorry, no args have been supplied")
        case h :: Nil => {
          val bids = Source.fromFile(h).getLines.toList.map {
            l =>
              val line = l.split(' ')
              Bid(line(0).charAt(0), line(1).toInt, line(2).toDouble)
          }
          calcAndPrintResult(bids)
        }
        case h :: t => {
          if ((h::t).length % 3 == 0) {
            val bids = (h::t).grouped(3).toList.map {
              a => Bid(a(0).charAt(0), a(1).toInt, a(2).toDouble)
            }
            calcAndPrintResult(bids)
          }
          else {
            throw new Exception("Please supply arguments number divisible by 3")
          }
        }
      }
    ) match {
      case Success(res) => res
      case Failure(f) => println(f)
    }

    System.exit(0)

  }

  val calcAndPrintResult: List[Bid] => Unit = bids => println(s"OUTPUT: ${EOL}${conductBidding(bids)}")

  def conductBidding(bids: List[Bid]): String = {

    val ex = Executors.newFixedThreadPool(EXECUTOR_POOL_SIZE)

    val bBids = bids.filter(_.direction == Bid.B)

    val sBids = bids.filter(_.direction == Bid.S)

    val buyPriceDist = priceDistAsynch(ex, splitBidGr(bBids))

    val sellPriceDist = priceDistAsynch(ex, splitBidGr(sBids))

    val optSellPricesByVolume =
      optSellPricesByVolumeAsync(ex, sellPriceDist.grouped(calcPartsLength(sellPriceDist)).toList, buyPriceDist)

    val resPriceByVolume = ResultVolumeByOrder(
      optSellPricesByVolume.optPrices.map(
        sPrByOrdNum =>
          (sPrByOrdNum.price, 1)
      ).reduce(
        (l: (Double, Int), r: (Double, Int)) => (l._1 + r._1, l._2 + l._2)
      ) |> {
        case (optPrSum: Double, optSlPrsNumber: Int) => optPrSum / optSlPrsNumber
      }
      , optSellPricesByVolume.totalVolume
      , optSellPricesByVolume.optPrices.map(_.quantity).reduce(_ + _)
    )

    resPriceByVolume match {
      case ResultVolumeByOrder(_, 0.0, _) => "0 n/a"
      case ResultVolumeByOrder(price, _, quantity) =>
        s"${quantity} ${BigDecimal.valueOf(price).setScale(2, BigDecimal.RoundingMode.HALF_UP)}"
    }

  }

  // splitting for asynch calculations
  private def splitBidGr(grBids: List[Bid]): List[List[Bid]] = {
    def go(bidsGrToSplit: List[Bid], grBorder: Double, splittedBidGroups: List[List[Bid]]): List[List[Bid]] = {
      bidsGrToSplit match {
        case h :: t =>
          go((h :: t).filterNot(_.price <= grBorder), grBorder + grBorder, (h :: t).filter(_.price <= grBorder) :: splittedBidGroups)
        case Nil => splittedBidGroups
      }
    }

    go(grBids, MAX_BID_PRICE / (EXECUTOR_POOL_SIZE.toDouble), Nil)
  }

  // asynch calculation of price by quantity of orders distribution, result is distinct by price
  private def priceDistAsynch(ex: ExecutorService, splittedBidGroups: List[List[Bid]]): List[PriceByOrderNum] = {
    splittedBidGroups
      .map(
        bidsGrPart =>
          new Callable[Either[List[PriceByOrderNum], Throwable]] {
            override def call(): Either[List[PriceByOrderNum], Throwable] = {
              Thread.currentThread().setName(s"groupBids ${System.currentTimeMillis()}")
              Try(groupBids(bidsGrPart)) match {
                case Success(res) => Left(res)
                case Failure(f) => Right(f)
              }

            }
          }
      )
      .map(ex.submit(_))
      .map(
        _.get match {
          case Left(res) => res
          case Right(f) => throw f
        }
      )
      .reduce(_ ::: _)
  }

  // grouping Bids by prices aggregating on quantity
  private def groupBids(bids: List[Bid]): List[PriceByOrderNum] = {
    @tailrec
    def go(bidsToGr: List[Bid], groupedBids: List[PriceByOrderNum]): List[PriceByOrderNum] = {
      bidsToGr match {
        case h :: t => {
          val prGr = h.price
          val totQuantity = (h :: t).filter(_.price == prGr).map(_.quantity).reduce(_ + _)
          go(
            t.filterNot(_.price == prGr)
            , PriceByOrderNum(prGr, totQuantity) :: groupedBids
          )
        }

        case Nil => groupedBids
      }
    }

    go(bids, Nil)
  }

  // calculating optimized length of PriceByOrdedNum list to split it for asynch processing
  private def calcPartsLength: List[PriceByOrderNum] => Int =
    initialList => if (initialList.length > SPLIT_FLAG) initialList.length / EXECUTOR_POOL_SIZE else initialList.length


  // async calculation of optSellPrice might come handy if number of unique groups (different prices) is great,
  // consider distributing 1M of values with upper value border 100K (test max value 100)
  private def optSellPricesByVolumeAsync(ex: ExecutorService, sellPricesByNumGr: List[List[PriceByOrderNum]],
                                         buyPriceDist: List[PriceByOrderNum]): OptSalePricesByVolume = {
    val optSellPricesByVolumeParts = sellPricesByNumGr
      .map {
        part =>
          new Callable[Either[OptSalePricesByVolume, Throwable]] {
            override def call(): Either[OptSalePricesByVolume, Throwable] = {
              Try(calcOptSalePrice(part, buyPriceDist)) match {
                case Success(res) => Left(res)
                case Failure(f) => Right(f)
              }
            }
          }
      }

   optSellPricesByVolumeParts
      .map(task => ex.submit(task))
      .map(_.get)
       .map(
         optSellPrAsynchRes => optSellPrAsynchRes match {
           case Left(res) => res
           case Right(f) => throw f
         }
       )
      .foldLeft[OptSalePricesByVolume](OptSalePricesByVolume(Nil, 0.0)) {
      (acc, p) =>
        p match {
          case optSelPr if optSelPr.totalVolume > acc.totalVolume =>
            OptSalePricesByVolume(optSelPr.optPrices, optSelPr.totalVolume)
          case optSelPr if optSelPr.totalVolume == acc.totalVolume =>
            OptSalePricesByVolume(optSelPr.optPrices ::: acc.optPrices, optSelPr.totalVolume + acc.totalVolume)
          case _ => acc
        }
    }

  }

  // get all optimum prices
  private def calcOptSalePrice: (List[PriceByOrderNum], List[PriceByOrderNum]) => OptSalePricesByVolume = {
    (sellPriceDist, buyPriceDist) =>
      sellPriceDist.foldLeft[OptSalePricesByVolume](OptSalePricesByVolume(Nil, 0.0)) {
        (acc, sPrByV) =>
          val posSlQ = calcPossibleSaleQuantity(sPrByV, buyPriceDist)
          posSlQ * sPrByV.price match {
            case sTotalVolume if sTotalVolume > acc.totalVolume =>
              OptSalePricesByVolume(PriceByOrderNum(sPrByV.price, posSlQ) :: Nil, sTotalVolume)
            case sTotalVolume if sTotalVolume == acc.totalVolume =>
              OptSalePricesByVolume(PriceByOrderNum(sPrByV.price, posSlQ) :: acc.optPrices, sTotalVolume + acc.totalVolume)
            case _ => acc
          }
      }
  }

  // calculate max possible sell amount (buy amount) bu certain price
  private def calcPossibleSaleQuantity: (PriceByOrderNum, List[PriceByOrderNum]) => Int = { (sPrByOrderNum, bPricesByOrderNum) =>
    (bPricesByOrderNum.foldLeft[(Int, Int)]((0, 0)) {
      case ((sQ, bQ), bPrByOrderNum) =>
        if (sPrByOrderNum.price <= bPrByOrderNum.price) (sQ + sPrByOrderNum.quantity, bQ + bPrByOrderNum.quantity) else (sQ, bQ)
    }) |> { case (sQ, bQ) => if (sQ > bQ) bQ else sQ }
  }

}