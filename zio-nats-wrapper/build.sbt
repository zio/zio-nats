ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "zio-nats",
    version := "0.1.0-SNAPSHOT",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.1.1",
      "dev.zio" %% "zio-streams" % "2.1.1",
      "io.nats" % "jnats" % "2.21.0",
      "dev.zio" %% "zio-test" % "2.1.1" % Test,
      "dev.zio" %% "zio-test-sbt" % "2.1.1" % Test
    )
  )
