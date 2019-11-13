package com.okg.state

sealed trait CoordinatorState extends State

case object WAIT_ALL extends CoordinatorState

case object GENERATION extends CoordinatorState
