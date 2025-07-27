ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.12"
ThisBuild / organization := "io.github.zio"

lazy val root = (project in file("."))
  .settings(
    name := "zio-nats",
    libraryDependencies ++= Seq(
      // ZIO dependencies
      "dev.zio" %% "zio" % "2.0.21",
      "dev.zio" %% "zio-streams" % "2.0.21",
      "dev.zio" %% "zio-concurrent" % "2.0.21",
      
      // NATS Java client
      "io.nats" % "jnats" % "2.21.4",
      
      // Testing dependencies
      "dev.zio" %% "zio-test" % "2.0.21" % Test,
      "dev.zio" %% "zio-test-sbt" % "2.0.21" % Test,
      "dev.zio" %% "zio-test-magnolia" % "2.0.21" % Test,
      
      // Logging
      "dev.zio" %% "zio-logging" % "2.1.16",
      "dev.zio" %% "zio-logging-slf4j" % "2.1.16",
      "ch.qos.logback" % "logback-classic" % "1.4.14"
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen",
      "-Ywarn-value-discard"
    )
  )
