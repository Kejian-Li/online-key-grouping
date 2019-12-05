package com.okg.actor

import akka.actor.{Actor, ActorRef, FSM}
import akka.japi.Option.Some
import com.okg.message._
import com.okg.message.communication.{MigrationCompleted, StartSimulation}
import com.okg.message.registration.{CompilerRegistrationAtInstance, StatisticsRegistrationAtCompiler}
import com.okg.message.statistics.CompilerStatistics
import com.okg.state._

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
  var nextRoutingTable = new RoutingTable(mutable.Map.empty[Int, Int]) // empty routing table

  startWith(WAIT_ALL, new CompilerStateData(
    new RoutingTable(mutable.Map.empty[Int, Int]),
    List.empty[Sketch],
    0,
    new Array[Int](k)))

  when(WAIT_ALL) {
    case Event(sketch: Sketch, compilerStateData: CompilerStateData) => {

      log.info("Compiler received sketch from " + sender())

      val newStateData = compilerStateData.copy(sketches = compilerStateData.sketches :+ sketch)
      log.info("Compiler received sketch " + newStateData.sketches.size + " in total")
      if (newStateData.sketches.size == s) {
        log.info("Compiler is gonna COMPILE")
        goto(COMPILE) using (newStateData)
      } else {
        stay() using (newStateData)
      }
    }
  }

  when(COMPILE) {
    case Event(MigrationCompleted, compilerStateData: CompilerStateData) => {
      compilerStateData.migrationCompletedNotifications += 1

      if (compilerStateData.migrationCompletedNotifications == k) {
        if (nextRoutingTable.isEmpty) { // sanity check
          log.error("Compiler: next routing table is empty")
        } else {
          log.info("Compiler: new routing table size is " + nextRoutingTable.size())
        }
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
      nextRoutingTable = generateRoutingTable(nextStateData)
      val generationEnd = System.nanoTime()
      val generationTime = generationEnd - generationStart

      log.info("Compiler: next routing table size is: " + nextRoutingTable.size)
      log.info("Compiler: next routing table:")
      nextRoutingTable.map.foreach {
        entry => {
          log.info(entry._1 + "  " + entry._2)
        }
      }

      val migrationTable = makeMigrationTable(nextRoutingTable)
      log.info("Compiler: make migration table successfully, its size is: " + migrationTable.size)
      log.info("Compiler: next migration table:")

      migrationTable.map.foreach {
        entry => {
          if (entry._2.before.isEmpty) {
            log.info(entry._1 + "  null  " + entry._2.after.get)
          } else if (entry._2.after.isEmpty) {
            log.info(entry._1 + "  " + entry._2.before.get + "  null")
          } else {
            log.info(entry._1 + "  " + entry._2.before.get + "  " + entry._2.after.get)
          }
        }
      }
      instanceActors.foreach(instanceActor => {
        instanceActor ! new StartMigration(instanceActors, migrationTable)
      })

      // send statistics
      statisticsActor ! new CompilerStatistics(period,
        Duration.fromNanos(generationTime).toMicros,
        nextRoutingTable.size(),
        migrationTable.size())
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

  var historicalBuckets: Array[Int] = null

  // build global mapping and return routing table
  // update historicalBuckets of CoordinatorStateData
  def buildGlobalMappingFunction(heavyHitters: Seq[(Int, Int)], //  (key, frequency)
                                 buckets: Array[Int],
                                 compilerStateData: CompilerStateData) = {
    historicalBuckets = compilerStateData.historicalBuckets.clone()
    for (i <- 0 to k - 1) {
      historicalBuckets.update(i, historicalBuckets(i) + buckets(i))
    }

    val heavyHittersMapping = mutable.Map.empty[Int, Int]
    heavyHitters.foreach(
      entry => {
        val key = entry._1
        val leastIndex = findLeastLoad(historicalBuckets)
        historicalBuckets.update(leastIndex, historicalBuckets(leastIndex) + entry._2)
        heavyHittersMapping.put(key, leastIndex)
      }
    )

    //    log.info("Compiler: historical buckets will be: ")
    //    for (i <- 0 to historicalBuckets.length - 1) {
    //      log.info(i + "  " + historicalBuckets(i))
    //    }

    new RoutingTable(heavyHittersMapping)
  }

  def cumulateSketches(compilerStateData: CompilerStateData) = {
    val cumulativeHeavyHittersMap = mutable.TreeMap.empty[Int, Int]
    val cumulativeBuckets = new Array[Int](k)

    //    log.info("Compiler: " + "heavy hitters of each original sketch: ")
    //    var i = 0
    //    compilerStateData.sketches.foreach {
    //      sketch => {
    //        log.info("Compiler: sketch " + i + "'s heavy hitters of original sketch: ")
    //        sketch.heavyHitters.foreach {
    //          entry => {
    //            log.info(entry._1 + "  " + entry._2)
    //          }
    //        }
    //        i += 1
    //      }
    //    }

    //    for (j <- 0 to compilerStateData.sketches.size - 1) {
    //      log.info("Compiler: sketch " + j + "'s buckets: ")
    //      for (i <- 0 to k - 1) {
    //        log.info(i + "  " + compilerStateData.sketches(j).buckets(i))
    //      }
    //    }

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

    //    log.info("Compiler: " + "cumulative heavy hitters in the original order: ")
    //    cumulativeHeavyHittersMap.foreach {
    //      entry => {
    //        log.info(entry._1 + "  " + entry._2)
    //      }
    //    }
    //
    //    log.info("Compiler: cumulative buckets:")
    //    for (i <- 0 to k - 1) {
    //      log.info(i + "  " + cumulativeBuckets(i))
    //    }

    new Sketch(cumulativeHeavyHittersMap, cumulativeBuckets)
  }

  // generate a new routing table
  def generateRoutingTable(compilerStateData: CompilerStateData): RoutingTable = {
    val cumulativeSketch = cumulateSketches(compilerStateData)
    val descendingHeavyHittersMap = cumulativeSketch.heavyHitters.toSeq.sortWith(_._2 > _._2)

    //    log.info("Compiler: " + "cumulative heavy hitters in the descending order: ")
    //    descendingHeavyHittersMap.foreach {
    //      entry => {
    //        log.info(entry._1 + "  " + entry._2)
    //      }
    //    }

    buildGlobalMappingFunction(descendingHeavyHittersMap, cumulativeSketch.buckets, compilerStateData)
  }

  // compare currentRoutingTable with nextRoutingTable to make migration table
  def makeMigrationTable(nextRoutingTable: RoutingTable) = {

    val migrationTable = new MigrationTable(mutable.Map.empty[Int, Pair])
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
