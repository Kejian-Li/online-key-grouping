package com.okg.message.communication

import akka.dispatch.ControlMessage

/**
  * Message inherited from {@Link ControlMessage} can be inserted into the head of mail-box's queue
  */
case object MigrationCompleted extends ControlMessage
