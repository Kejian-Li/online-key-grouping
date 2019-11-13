package com.akka.inventory

sealed trait State

case object WaitForRequest extends State

case object ProcessRequest extends State

case object WaitForPublisher extends State

case object SoldOut extends State

case object ProcessSoldOut extends State
