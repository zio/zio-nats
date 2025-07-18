import BuildHelper._

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://github.com/zio/zio-nats")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "contributors",
        "Contributors",
        "",
        url("https://github.com/zio/zio-nats/graphs/contributors")
      )
    ),
    scalaVersion := "2.13.8",
    crossScalaVersions := Seq("2.12.15", "2.13.8", "3.1.3"),
    resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

val zioVersion = "2.0.5"

lazy val root = project
  .in(file("."))
  .settings(
    name := "zio-nats",
    stdSettings("zio-nats"),
    libraryDependencies ++= Seq(
      // ZIO
      "dev.zio" %% "zio"          % zioVersion,
      "dev.zio" %% "zio-streams"  % zioVersion,
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
      
      // NATS Java Client
      "io.nats" % "jnats" % "2.16.8",
      
      // For logging
      "org.slf4j" % "slf4j-api" % "2.0.5",
      "ch.qos.logback" % "logback-classic" % "1.4.5" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
