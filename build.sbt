inThisBuild(List(
  organization := "io.github.scoquelin",
  homepage := Some(url("https://github.com/scoquelin/arugula")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      "scoquelin",
      "Sébastien Coquelin",
      "seb.coquelin at gmail.com",
      url("https://www.github.com/scoquelin")
    ),
    Developer(
      "72squared",
      "John Loehrer",
      "72squared at gmail.com",
      url("https://www.github.com/72squared")
    )
  )
))

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"
ThisBuild / crossScalaVersions := Seq("2.13.14", "3.3.3")

lazy val root = (project in file("."))
  .settings(
    name := "arugula"
  )
  .aggregate(api, core)

lazy val api = (project in file ("modules/api"))
  .settings(
    name := "arugula-api"
  )

lazy val core = (project in file ("modules/core"))
  .settings(
    name := "arugula-core",
    libraryDependencies ++= Seq(
      "io.lettuce" % "lettuce-core" % "6.3.+",
    )
  )
  .dependsOn(api)

lazy val unitTests = (project in file ("modules/tests/test"))
  .settings(
    name := "arugula-unit-tests",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.mockito" % "mockito-core" % "5.12.0" % Test,
      "org.scalatestplus" %% "mockito-5-12" % "3.2.19.0" % Test,
      "ch.qos.logback" % "logback-classic" % "1.5.6" % Test,
    )
  )
  .dependsOn(api, core)

lazy val integrationTests = (project in file ("modules/tests/it"))
  .settings(
    name := "arugula-integration-tests",
    fork := true,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "com.dimafeng" %% "testcontainers-scala" % "0.41.4" % Test,
      "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.41.4" % Test,
      "ch.qos.logback" % "logback-classic" % "1.5.6" % Test,
    )
  )
  .dependsOn(api, core)

addCommandAlias("test", ";unitTests/test")
addCommandAlias("testAll", ";unitTests/test;integrationTests/test")

