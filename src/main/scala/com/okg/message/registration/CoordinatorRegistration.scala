package com.okg.message.registration

import akka.dispatch.ControlMessage

/**
  * Message used by CoordinatorActor to register itself at the instances of the operator.
  * It is inherited from {@Link ControlMessage} and can be inserted into the head of mail-box's queue.
  */
object CoordinatorRegistration extends ControlMessage
