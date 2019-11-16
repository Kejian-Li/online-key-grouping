package com.akka.inventory.state

sealed trait State

case object WaitForRequests extends State

case object ProcessRequest extends State

case object WaitForPublisher extends State

case object SoldOut extends State

case object ProcessSoldOut extends State
