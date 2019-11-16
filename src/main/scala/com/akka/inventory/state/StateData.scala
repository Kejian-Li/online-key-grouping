package com.akka.inventory.state

import com.akka.inventory.events.BookRequest

case class StateData(nrBooksInStore: Int, pendingRequests: Seq[BookRequest])
