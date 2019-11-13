package com.akka.inventory

import akka.actor.Actor

class Publisher(totalNrBooks: Int, nrBooksPerRequest: Int) extends Actor {

  var nrLeft = totalNrBooks

  override def receive: Receive = {
    case PublishRequest => {
      if (nrLeft == 0) {
        sender() ! BookSupplySoldOut
      } else {
        val supply = min(nrBooksPerRequest, nrLeft)
        nrLeft -= supply
        sender() ! new BookSupply(supply)
      }
    }
  }

  def min(x: Int, y: Int): Int = {
    if (x <= y) {
      x
    } else {
      y
    }
  }
}
