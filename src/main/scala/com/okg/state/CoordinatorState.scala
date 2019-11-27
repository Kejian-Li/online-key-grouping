package com.okg.state

sealed trait CoordinatorState extends State

/**
  * wait all schedulers
  */
case object WAIT_ALL extends CoordinatorState

case object GENERATION extends CoordinatorState
