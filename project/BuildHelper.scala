import sbt._
import sbt.Keys._

object BuildHelper {
  val stdSettings = Def.settings(
    scalacOptions := Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-feature",
      "-unchecked",
      "-Xfatal-warnings",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-language:existentials"
    ) ++ (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) =>
        Seq(
          "-Xsource:2.13",
          "-Yno-adapted-args",
          "-Ypartial-unification",
          "-Ywarn-dead-code",
          "-Ywarn-numeric-widen",
          "-Ywarn-value-discard"
        )
      case Some((2, 13)) =>
        Seq(
          "-Wunused:imports",
          "-Wvalue-discard",
          "-Wunused:locals",
          "-Wunused:patvars",
          "-Wunused:privates"
        )
      case Some((3, _)) =>
        Seq(
          "-source:3.0-migration",
          "-Xignore-scala2-macros"
        )
      case _ => Seq.empty
    }),
    Test / parallelExecution := false
  )
}
