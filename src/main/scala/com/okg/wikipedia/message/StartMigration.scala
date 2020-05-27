package com.okg.wikipedia.message

import akka.actor.ActorRef

/**
  *
  * @param instanceActors actors' reference of parallel instances for migrating keys with frequencies (i.e., states)
  * @param migrationTable migration table
  */
case class StartMigration(instanceActors: Array[ActorRef], migrationTable: MigrationTable)
