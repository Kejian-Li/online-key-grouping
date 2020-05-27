package com.okg.wikipedia.message.communication

import akka.dispatch.ControlMessage
import com.okg.wikipedia.message.Sketch

case class LearnCompleted(sketch: Sketch) extends ControlMessage