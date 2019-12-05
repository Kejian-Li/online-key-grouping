package com.okg.message.registration

import akka.dispatch.ControlMessage

/**
  * Message used by StatisticsActor to register itself at the InstanceActor.
  * It is inherited from {@Link ControlMessage} and can be inserted into the head of mail-box's queue.
  */
object StatisticsRegistrationAtInstance extends ControlMessage
