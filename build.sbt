ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

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
      "ch.qos.logback" % "logback-classic" % "1.5.6" % Test
    )
  )
  .dependsOn(api, core)

addCommandAlias("testAll", ";unitTests/test;integrationTests/test")

