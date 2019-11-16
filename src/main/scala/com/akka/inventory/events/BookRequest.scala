package com.akka.inventory.events

import akka.actor.ActorRef

case class BookRequest(context: AnyRef, target: ActorRef)
