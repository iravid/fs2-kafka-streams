import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.6",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "fs2-kafka-streams",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "1.1.0",
      "org.typelevel" %% "cats-core" % "1.1.0",
      "org.typelevel" %% "cats-effect" % "0.10.1",
      "co.fs2" %% "fs2-core" % "0.10.4"
    ),
    scalacOptions += "-Ypartial-unification"
  )
