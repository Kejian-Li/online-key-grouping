package com.akka.inventory.response

case class BookReply(context: AnyRef, reserveId: Either[String, Int])
