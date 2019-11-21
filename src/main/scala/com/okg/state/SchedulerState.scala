package com.okg.state

sealed trait SchedulerState extends State

case object HASH extends SchedulerState

case object COLLECT extends SchedulerState

case object LEARN extends SchedulerState

case object WAIT extends SchedulerState

case object ASSIGN extends SchedulerState
