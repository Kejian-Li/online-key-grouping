package com.akka.inventory

import akka.actor.{Actor, ActorRef, FSM}
import com.akka.inventory.events._
import com.akka.inventory.response.{BookReply, PublisherRequest}
import com.akka.inventory.state._

class Inventory(publisher: ActorRef) extends Actor with FSM[State, StateData] {

  var reserveId = 0

  startWith(WaitForRequests, new StateData(0, Seq()))

  when(WaitForRequests) {
    case Event(request: BookRequest, stateData: StateData) => {
      val newStateData = stateData.copy(pendingRequests = stateData.pendingRequests :+ request)
      if (newStateData.nrBooksInStore > 0) {
        goto(ProcessRequest) using (newStateData)
      } else {
        goto(WaitForPublisher) using (newStateData)
      }
    }
    case Event(PendingRequests, stateData: StateData) => {
      if (stateData.pendingRequests.isEmpty) {
        stay()
      } else if (stateData.nrBooksInStore > 0) {
        goto(ProcessRequest)
      } else {
        goto(WaitForPublisher)
      }
    }
  }

  when(WaitForPublisher) {
    case Event(supply: BookSupply, stateData: StateData) => {
      goto(ProcessRequest) using (stateData.copy(nrBooksInStore = supply.nrBooks))
    }
    case Event(BookSupplySoldOut, _) => {
      goto(ProcessSoldOut)
    }
  }

  when(ProcessRequest) {
    case Event(Done, stateData: StateData) => {
      goto(WaitForRequests) using (stateData.copy(nrBooksInStore = stateData.nrBooksInStore - 1,
        pendingRequests = stateData.pendingRequests.tail))
    }
  }

  when(SoldOut) {
    case Event(request: BookRequest, stateData: StateData) => {
      goto(ProcessSoldOut) using (new StateData(0, Seq(request)))
    }
  }

  when(ProcessSoldOut) {
    case Event(Done, stateData: StateData) => {
      goto(SoldOut) using (new StateData(0, Seq()))
    }
  }

  //common code for all states
  whenUnhandled {
    case Event(request: BookRequest, stateData: StateData) => {
      stay() using (stateData.copy(pendingRequests = stateData.pendingRequests :+ request))
    }
    case Event(e, s) => {
      log.warning("received unhandled request{} in state{} / {}", e, stateName, s)
      stay()
    }
  }

  onTransition {
    case _ -> WaitForRequests => {
      if (!nextStateData.pendingRequests.isEmpty) {
        self ! PendingRequests
      }
    }
    case _ -> WaitForPublisher => {
      publisher ! PublisherRequest
    }
    case _ -> ProcessRequest => {
      val request = nextStateData.pendingRequests.head
      reserveId += 1
      request.target ! new BookReply(request.context, Right(reserveId))
      self ! Done
    }
    case _ -> ProcessSoldOut => {
      nextStateData.pendingRequests.foreach(request => {
        request.target ! new BookReply(request.context, Left("Sold out"))
      })
      self ! Done
    }
  }

  initialize()

}
