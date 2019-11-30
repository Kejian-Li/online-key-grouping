package com.okg.message.communication

import akka.dispatch.ControlMessage
import com.okg.message.Sketch

case class LearnCompleted(sketch: Sketch) extends ControlMessage