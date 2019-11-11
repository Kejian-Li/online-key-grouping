
name := "online-key-grouping"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies ++= Seq (
  "com.typesafe.akka" %% "akka-actor" % "2.5.23",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.23" % "test",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "com.github.rokishopen" % "random-people" % "0.0.1"
)