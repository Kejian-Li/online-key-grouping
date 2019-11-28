
name := "online-key-grouping"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies ++= Seq (
  "com.typesafe.akka" %% "akka-actor" % "2.5.10",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.10" % "test",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.apache.commons" % "commons-math3" % "3.6.1",
  "net.sourceforge.javacsv" % "javacsv" % "2.0",
  "com.google.guava" % "guava" % "23.0"
)