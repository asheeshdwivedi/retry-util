name := "retry-util"

version := "0.1"

scalaVersion := "2.13.2"

lazy val akkaVersion = "2.6.3"
lazy val scalaTestVersion = "3.1.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)
