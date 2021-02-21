name := "datastore"
version := "0.1"
scalaVersion := "2.12.11"

// DSL parsing
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"

// Akka system
val akkaVersion = "2.6.4"
libraryDependencies += "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test

// Akka HTTP for JSON objects
// https://doc.akka.io/docs/akka-http/current/common/json-support.html
val AkkaHttpVersion = "10.2.3"
libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion

// Database support
libraryDependencies += "com.h2database" % "h2" % "1.4.200"

// Testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"

// Logging facilities
// https://github.com/lightbend/scala-logging
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

