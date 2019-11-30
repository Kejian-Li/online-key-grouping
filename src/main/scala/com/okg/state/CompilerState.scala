package com.okg.state

sealed trait CompilerState extends State

/**
  * wait all schedulers
  */
case object WAIT_ALL extends CompilerState

case object COMPILE extends CompilerState
