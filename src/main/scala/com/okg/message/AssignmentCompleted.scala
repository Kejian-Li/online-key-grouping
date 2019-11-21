package com.okg.message

import akka.dispatch.ControlMessage

/**
  * Message inherited from {@Link ControlMessage} can be inserted into the head of mail-box's queue
  */
object AssignmentCompleted extends ControlMessage