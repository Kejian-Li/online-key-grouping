package com.okg.message

import akka.actor.ActorRef
import akka.dispatch.ControlMessage

/**
  * Message inherited from {@Link ControlMessage} can be inserted into the head of mail-box's queue
  *
  * @param instanceActors actors' reference of parallel instances for migrating keys with frequencies (i.e., states)
  * @param migrationTable migration table
  */
case class StartMigration(instanceActors: Array[ActorRef], migrationTable: MigrationTable) extends ControlMessage
