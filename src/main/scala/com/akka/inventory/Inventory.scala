package com.akka.inventory

import akka.actor.{Actor, ActorRef, FSM}

class Inventory(publisher: ActorRef) extends Actor with FSM[State, StateData] {

  startWith(WaitForRequest, new StateData(0, Seq()))

  when(WaitForRequest) {
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
      goto(WaitForRequest) using (stateData.copy(nrBooksInStore = stateData.nrBooksInStore - 1,
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

  onTransition {
    case _ -> WaitForRequest => {
      if (!nextStateData.pendingRequests.isEmpty) {
        self ! PendingRequests
      }
    }
    case _ -> WaitForPublisher => {
      publisher ! PublishRequest
    }
    case _ -> ProcessRequest => {
      val request = nextStateData.pendingRequests.head
      reservedId += 1
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
