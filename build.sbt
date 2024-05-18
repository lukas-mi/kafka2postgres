ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/com.typesafe/config
  "com.typesafe" % "config" % "1.4.3",
  // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
  "org.slf4j" % "slf4j-api" % "2.0.13",
  // https://mvnrepository.com/artifact/ch.qos.logback/logback-classic
  "ch.qos.logback" % "logback-classic" % "1.5.5",

  // https://mvnrepository.com/artifact/org.apache.avro/avro
  "org.apache.avro" % "avro" % "1.11.3",

  // https://mvnrepository.com/artifact/org.postgresql/postgresql
  "org.postgresql" % "postgresql" % "42.7.3",

  // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
  "org.apache.kafka" % "kafka-clients" % "3.7.0",

  // https://mvnrepository.com/artifact/org.scalatest/scalatest
  "org.scalatest" %% "scalatest" % "3.2.18" % Test,
  // https://mvnrepository.com/artifact/org.testcontainers/testcontainers
  "org.testcontainers" % "testcontainers" % "1.19.8" % Test,
  // https://mvnrepository.com/artifact/org.testcontainers/kafka
  "org.testcontainers" % "kafka" % "1.19.8" % Test,
  // https://mvnrepository.com/artifact/org.testcontainers/postgresql
  "org.testcontainers" % "postgresql" % "1.19.8" % Test
)

lazy val root = (project in file("."))
  .settings(
    name := "com/mikelionis/lukas/kafka2postgres"
  )
