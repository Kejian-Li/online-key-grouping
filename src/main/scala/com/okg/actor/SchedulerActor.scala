package com.okg.actor

import java.util

import akka.actor.{Actor, ActorRef, FSM}
import com.okg.message._
import com.okg.message.communication._
import com.okg.message.registration.StatisticsRegistrationAtScheduler
import com.okg.message.statistics.SchedulerStatistics
import com.okg.state._
import com.okg.tuple.{Tuple, TupleQueue}
import com.okg.util.{SpaceSaving, TwoUniversalHash}
import org.apache.commons.math3.random.RandomDataGenerator

/**
  * Actor for Scheduler instance
  */

import scala.collection.mutable

class SchedulerActor(index: Int, // index of this Scheduler instance
                     m: Int, // number of involved tuples in each period
                     k: Int, // number of Operator instances
                     epsilon: Double,
                     theta: Double,
                     compilerActor: ActorRef,
                     instanceActors: Array[ActorRef]) extends Actor with FSM[SchedulerState, SchedulerStateData] {

  var hashFunction: TwoUniversalHash = null
  var statisticsActor = ActorRef.noSender

  //initialize
  override def preStart(): Unit = {
    instantiateHashFunction()
  }

  startWith(COLLECT, initializeSchedulerStateDate())

  private def initializeSchedulerStateDate() = {
    new SchedulerStateData(
      m,
      k,
      new SpaceSaving(epsilon, theta),
      new Array[Int](k),
      new RoutingTable(mutable.Map.empty[Int, Int]),
      new TupleQueue[Tuple[Int]])
  }

  private def instantiateHashFunction() = {
    val uniformGenerator = new RandomDataGenerator()
    uniformGenerator.reSeed(1000)

    val prime = 10000019L
    val a = uniformGenerator.nextLong(1, prime - 1)
    val b = uniformGenerator.nextLong(1, prime - 1)
    hashFunction = new TwoUniversalHash(k, prime, a, b)
  }

  def hash(key: Int) = {
    hashFunction.hash(key)
  }

  def assignTuple(tuple: Tuple[Int], routingTable: RoutingTable) = {
    var targetIndex = -1
    val key = tuple.key
    if (routingTable.contains(key)) {
      targetIndex = routingTable.get(key)
    } else {
      targetIndex = hash(key)
    }
    //        targetIndex = hash(key)
    instanceActors(targetIndex) ! tuple
  }

  // from java's HashMap to Scala's mutable.Map
  // buckets subtract heavy hitters
  def makeSketch(rawHeavyHitters: util.HashMap[Integer, Integer], buckets: Array[Int]) = {
    val it = rawHeavyHitters.entrySet().iterator()
    val heavyHitters = new mutable.HashMap[Int, Int]()

    while (it.hasNext) {
      val entry = it.next()
      heavyHitters.put(entry.getKey, entry.getValue)
      val targetIndex = hash(entry.getKey)
      buckets.update(targetIndex, buckets(targetIndex) - entry.getValue)
    }

    new Sketch(index, heavyHitters.clone(), buckets.clone())
  }

  var assignedTotalTuplesNum = 0
  var period = 1

  var periodStart: Long = 0    // LEARN
  var periodEnd: Long = 0      // WAIT
  var periodDelay: Long = periodEnd - periodStart   // time of LEARN + WAIT

  when(COLLECT) {
    case Event(tuple: Tuple[Int], schedulerStateData: SchedulerStateData) => {
      schedulerStateData.tupleQueue += tuple
      if (schedulerStateData.tupleQueue.size >= m) {
        log.info("Scheduler " + index + " collects successfully")
        periodStart = System.nanoTime()
        goto(LEARN)
      } else {
        stay()
      }
    }

  }

  var i = 0

  def learn(schedulerStateData: SchedulerStateData) = {
    val tupleQueue = schedulerStateData.tupleQueue

    // learn
    val it = tupleQueue.iterator
    while (i < m) {
      val tuple = it.next() // return but don't remove
      val key = tuple.key
      schedulerStateData.spaceSaving.newSample(key)
      val targetIndex = hash(key)
      schedulerStateData.buckets.update(targetIndex, schedulerStateData.buckets(targetIndex) + 1)
      i += 1
    }
    // make sketch
    val sketch = makeSketch(schedulerStateData.spaceSaving.getHeavyHitters, schedulerStateData.buckets)
    //check sketch
    var tuplesInSketch = 0
    tuplesInSketch += sketch.buckets.sum
    sketch.heavyHitters.foreach {
      entry => {
        tuplesInSketch += entry._2
      }
    }
    log.info("Scheduler " + index + " learns " + tuplesInSketch)
    assert(tuplesInSketch == m)

    sketch
  }

  when(LEARN) {

    case Event(learnCompleted: LearnCompleted, schedulerStateData: SchedulerStateData) => {

      // learn successfully and send sketch
      compilerActor ! learnCompleted.sketch

      log.info("Scheduler " + index + " sends sketch successfully")

      goto(WAIT) using (schedulerStateData.copy(spaceSaving = new SpaceSaving(epsilon, theta),
        buckets = new Array[Int](k)))
    }

  }

  var totalPeriodTime: Long = 0
  when(WAIT) {
    case Event(startAssignment: StartAssignment, schedulerStateData: SchedulerStateData) => {
      log.info("Scheduler " + index + " received routing table, starting assignment...")

      periodEnd = System.nanoTime()
      periodDelay = periodEnd - periodStart
      totalPeriodTime += periodDelay
      goto(ASSIGN) using (schedulerStateData.copy(routingTable = startAssignment.routingTable))
    }
  }

  var terminateSimulationFromSimulation = false
  var simulatorActor = ActorRef.noSender
  var tupleClear = false
  whenUnhandled {
    case Event(StartSimulation, schedulerStateData: SchedulerStateData) => {
      compilerActor ! StartSimulation

      for (i <- 0 to k - 1) {
        instanceActors(i) ! StartSimulation
      }
      stay()
    }

    case Event(tuple: Tuple[Int], schedulerStateData: SchedulerStateData) => {
      schedulerStateData.tupleQueue += tuple
      stay()
    }

    case Event(TerminateSimulation, schedulerStateData: SchedulerStateData) => {
      log.info("Scheduler " + index + " received termination notification")
      log.info("Scheduler " + index + " has " + schedulerStateData.tupleQueue.size + " unassigned tuples.........................................")
      terminateSimulationFromSimulation = true
      simulatorActor = sender()

      val totalPeriod = period
      val averageDelayTime = totalPeriodTime / totalPeriod
      simulatorActor ! new SchedulerStatistics(index, totalPeriod, averageDelayTime)

      if (tupleClear) {
        for (i <- 0 to k - 1) {
          // send termination notification to instances and assign receiver of respond as simulatorActor
          instanceActors(i).tell(TerminateSimulation, simulatorActor)
        }
        log.info("Scheduler " + index + " sends termination notification to all the InstanceActors")
      }

      stay()
    }

    case Event(TupleQueueClear, schedulerStateData: SchedulerStateData) => {
      tupleClear = true
      if (terminateSimulationFromSimulation) {
        for (i <- 0 to k - 1) {
          // send termination notification to instances and assign receiver of respond as simulatorActor
          instanceActors(i).tell(TerminateSimulation, simulatorActor)
        }
        log.info("Scheduler " + index + " sends termination notification to all the InstanceActors")
      }
      stay()
    }

    case Event(StatisticsRegistrationAtScheduler, schedulerStateData: SchedulerStateData) => {
      statisticsActor = sender()
      stay()
    }

  }

  def assignPeriodBarriers() = {
    instanceActors.foreach {
      instanceActor => {
        instanceActor ! new PeriodBarrier(index, period)
      }
    }
  }

  def assign(tupleQueue: TupleQueue[Tuple[Int]], routingTable: RoutingTable) = {
    var x = 0
    while (x < m) {
      assignedTotalTuplesNum += 1
      val tuple = tupleQueue.dequeue() // return and remove first element
      assignTuple(tuple, routingTable)
      x += 1
    }
    assignPeriodBarriers()
  }

  when(ASSIGN) {
    case Event(AssignmentCompleted, schedulerStateData: SchedulerStateData) => {
      period += 1
      log.info("Scheduler " + index + " assigns completely")
      if (schedulerStateData.tupleQueue.size >= m) {
        goto(LEARN)
      } else {
        val collectedTuplesNum = schedulerStateData.tupleQueue.size
        log.info("Scheduler " + index + " collects " + collectedTuplesNum + " tuples, it is not enough")
        if (collectedTuplesNum == 0) {
          self ! TupleQueueClear
        }
        goto(COLLECT)
      }
    }
  }

  onTransition {
    case _ -> COLLECT => {
      log.info("Scheduler " + index + " enters COLLECT")
    }

    case _ -> LEARN => {
      log.info("Scheduler " + index + " enters LEARN at period " + period)
      log.info("Scheduler " + index + " assigned so far " + assignedTotalTuplesNum + " tuples in total")

      i = 0

      val sketch = learn(nextStateData)
      self ! new LearnCompleted(sketch)
    }

    case _ -> ASSIGN => {
      log.info("Scheduler " + index + " enters ASSIGN")
      assign(nextStateData.tupleQueue, nextStateData.routingTable)

      self ! AssignmentCompleted // assignment in each period is completed
    }

    case _ -> WAIT => {
      log.info("Scheduler " + index + " enters WAIT")
    }
  }

}


