package ru.mb.bidding

import org.scalatest.{Matchers, WordSpecLike}

class BiddingLogicTest extends WordSpecLike with Matchers {

  "failed bidding" in {

    val inputData = List(
      Bid.buyOrderProt.copy(quantity = 100, price = 15.40),
      Bid.buyOrderProt.copy(quantity = 100, price = 15.30),
      Bid.buyOrderProt.copy(quantity = 100, price = 15.30),
      Bid.buyOrderProt.copy(quantity = 100, price = 15.30),
      Bid.buyOrderProt.copy(quantity = 100, price = 15.30),
      Bid.selOrderProt.copy(quantity = 100, price = 15.50),
      Bid.selOrderProt.copy(quantity = 100, price = 15.60)
    )

    Auction.conductBidding(inputData) shouldBe "0 n/a"

  }

  "one best price bidding" in {

    val inputData = List(
      Bid.buyOrderProt.copy(quantity = 100, price = 15.40),
      Bid.buyOrderProt.copy(quantity = 100, price = 15.30),
      Bid.buyOrderProt.copy(quantity = 100, price = 15.30),
      Bid.buyOrderProt.copy(quantity = 100, price = 15.30),
      Bid.buyOrderProt.copy(quantity = 100, price = 15.30),
      Bid.selOrderProt.copy(quantity = 200, price = 15.40)
    )

    Auction.conductBidding(inputData) shouldBe "100 15.40"

  }

  "two best prices bidding" in {

    val inputData = List(
      Bid.buyOrderProt.copy(quantity = 100, price = 50.00),
      Bid.selOrderProt.copy(quantity = 10, price = 20.00),
      Bid.selOrderProt.copy(quantity = 20, price = 10.00)
    )

    Auction.conductBidding(inputData) shouldBe "30 15.00"

  }

}

class BiddingHighLoadTest extends WordSpecLike with Matchers {
  "test 1M entries" in {
    println(Auction.conductBidding(genTestData(1000000)))
  }
}

