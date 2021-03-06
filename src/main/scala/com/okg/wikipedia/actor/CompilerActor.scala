package com.okg.wikipedia.actor

import akka.actor.{Actor, ActorRef, FSM}
import akka.japi.Option.Some
import com.okg.wikipedia.message._
import com.okg.wikipedia.message.communication.{MigrationCompleted, StartSimulation}
import com.okg.wikipedia.message.registration.{CompilerRegistrationAtInstance, StatisticsRegistrationAtCompiler}
import com.okg.wikipedia.message.statistics.CompilerStatistics
import com.okg.wikipedia.state._

import scala.collection.mutable
import scala.concurrent.duration.Duration

/**
  * Actor for Compiler
  *
  * @param schedulerActors
  * @param instanceActors
  * @param s
  * @param k
  */
case class CompilerActor(s: Int, // number of Scheduler instances
                         instanceActors: Array[ActorRef])
  extends Actor with FSM[CompilerState, CompilerStateData] {

  val k = instanceActors.size
  val schedulerActorsSet = mutable.Set.empty[ActorRef]
  var nextRoutingTable = new RoutingTable(mutable.Map.empty[String, Int]) // empty routing table

  startWith(WAIT_ALL, new CompilerStateData(
    new RoutingTable(mutable.Map.empty[String, Int]),
    List.empty[Sketch],
    0,
    new Array[Int](k)))

  when(WAIT_ALL) {
    case Event(sketch: Sketch, compilerStateData: CompilerStateData) => {

      log.info("Compiler received the sketch from Scheduler " + sketch.index)

      // log sketch
      log.info("sketch's heavy hitters are:")
      sketch.heavyHitters.foreach {
        entry => {
          log.info(entry._1 + " " + entry._2)
        }
      }
      log.info("sketch's buckets are:")
      var i = 0
      sketch.buckets.foreach {
        bucket => {
          log.info(i + " " + bucket)
          i += 1
        }
      }

      val newStateData = compilerStateData.copy(sketches = compilerStateData.sketches :+ sketch)
      if (newStateData.sketches.size == s) {
        log.info("Compiler received all the sketches and is gonna COMPILE")
        goto(COMPILE) using (newStateData)
      } else {
        stay() using (newStateData)
      }
    }
  }

  var historicalBuckets: Array[Int] = null

  when(COMPILE) {
    case Event(MigrationCompleted, compilerStateData: CompilerStateData) => {
      compilerStateData.migrationCompletedNotifications += 1

      if (compilerStateData.migrationCompletedNotifications == k) {

        log.info("Compiler: new routing table size is " + nextRoutingTable.size())

        goto(WAIT_ALL) using (
          new CompilerStateData(nextRoutingTable, List.empty[Sketch],
            0, historicalBuckets))
      } else {
        stay()
      }
    }
  }

  var statisticsActor = ActorRef.noSender
  whenUnhandled {
    case Event(StartSimulation, compilerStateData: CompilerStateData) => {
      schedulerActorsSet.add(sender())

      if (schedulerActorsSet.size == s) {
        for (i <- 0 to k - 1) {
          log.info("Compiler: register myself at the instance " + i + " of the Operator")
          instanceActors(i) ! CompilerRegistrationAtInstance
        }
      }
      stay()
    }
    case Event(StatisticsRegistrationAtCompiler, compilerStateData: CompilerStateData) => {
      statisticsActor = sender()
      stay()
    }
  }

  var period = 0
  onTransition {
    case WAIT_ALL -> COMPILE => {
      period += 1
      log.info("Compiler enters COMPILE " + "at period " + period)

      val generationStart = System.nanoTime()
      nextRoutingTable = compile(nextStateData)
      val generationEnd = System.nanoTime()

      val migrationTable = makeMigrationTable(nextRoutingTable)

      log.info("Compiler: make migration table successfully, its size is: " + migrationTable.size)

      instanceActors.foreach(instanceActor => {
        instanceActor ! new StartMigration(instanceActors, migrationTable)
      })

      // send statistics
      val generationTime = generationEnd - generationStart
      statisticsActor ! new CompilerStatistics(period, Duration.fromNanos(generationTime).toMicros,
        nextRoutingTable.size(), migrationTable.size())
    }

    case COMPILE -> WAIT_ALL => {
      log.info("Compiler enters WAIT_ALL")
      schedulerActorsSet.foreach(schedulerActor => {
        schedulerActor ! new StartAssignment(nextRoutingTable)
      })
    }
  }

  def findLeastLoad(buckets: Array[Int]): Int = {
    var minLoad = Int.MaxValue
    var minIndex = -1
    for (i <- 0 to buckets.length - 1) {
      if (buckets(i) < minLoad) {
        minLoad = buckets(i)
        minIndex = i
      }
    }
    minIndex
  }


  def aggregateAndSort(compilerStateData: CompilerStateData) = {
    val cumulativeHeavyHittersMap = mutable.TreeMap.empty[String, Int]
    val cumulativeBuckets = new Array[Int](k)

    compilerStateData.sketches.foreach(
      sketch => {

        sketch.heavyHitters.foreach(
          entry => {
            val lastValue = cumulativeHeavyHittersMap.getOrElse(entry._1, 0)
            cumulativeHeavyHittersMap.update(entry._1, lastValue + entry._2)
          }
        )

        for (i <- 0 to k - 1) {
          cumulativeBuckets(i) += sketch.buckets(i)
        }
      }
    )

    cumulativeHeavyHittersMap.toSeq.sortWith(_._2 > _._2)

    new Sketch(-1, cumulativeHeavyHittersMap, cumulativeBuckets)
  }

  // generate a new routing table
  def compile(compilerStateData: CompilerStateData): RoutingTable = {
    val cumulativeSketch = aggregateAndSort(compilerStateData)

    historicalBuckets = compilerStateData.historicalBuckets.clone()
    for (i <- 0 to k - 1) {
      historicalBuckets.update(i, historicalBuckets(i) + cumulativeSketch.buckets(i))
    }

    val heavyHittersMapping = mutable.Map.empty[String, Int]
    cumulativeSketch.heavyHitters.foreach(
      entry => {
        val key = entry._1
        val leastIndex = findLeastLoad(historicalBuckets)
        historicalBuckets.update(leastIndex, historicalBuckets(leastIndex) + entry._2)
        heavyHittersMapping.put(key, leastIndex)
      }
    )

    new RoutingTable(heavyHittersMapping)
  }

  // compare currentRoutingTable with nextRoutingTable to make migration table
  def makeMigrationTable(nextRoutingTable: RoutingTable) = {

    val migrationTable = new MigrationTable(mutable.Map.empty[String, Pair])
    val currentRoutingTable = nextStateData.currentRoutingTable

    currentRoutingTable.map.foreach {
      entry => {
        val key = entry._1
        if (nextRoutingTable.contains(key)) { // 1. both contain
          val after = nextRoutingTable.get(key)
          val pair = new Pair(Some(entry._2), Some(after))

          migrationTable.put(key, pair)
        } else { // 2. the old contains but the new doesn't
          migrationTable.put(key, new Pair(Some(entry._2), None))
        }
      }
    }

    nextRoutingTable.map.foreach {
      entry => {
        val key = entry._1
        if (!currentRoutingTable.contains(key)) { // 3. the new contains but the old doesn't
          migrationTable.put(key, new Pair(None, Some(entry._2)))
        }
      }
    }

    migrationTable
  }
}