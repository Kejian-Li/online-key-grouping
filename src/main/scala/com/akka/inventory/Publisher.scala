package com.akka.inventory

import akka.actor.Actor
import com.akka.inventory.events.{BookSupply, BookSupplySoldOut}
import com.akka.inventory.response.PublisherRequest

class Publisher(totalNrBooks: Int, nrBooksPerRequest: Int) extends Actor {

  var nrLeft = totalNrBooks

  override def receive: Receive = {
    case PublisherRequest => {
      if (nrLeft == 0) {
        sender() ! BookSupplySoldOut
      } else {
        val supply = math.min(nrBooksPerRequest, nrLeft)
        nrLeft -= supply
        sender() ! new BookSupply(supply)
      }
    }
  }

}
