package com.akka.inventory

case class StateData(nrBooksInStore: Int, pendingRequests: Seq[BookRequest]) {

}
