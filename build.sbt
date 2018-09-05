import Dependencies._

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(inConfig(IntegrationTest)(ScalafmtPlugin.scalafmtConfigSettings))
  .settings(
    inThisBuild(
      List(
        organization := "com.iravid",
        scalaVersion := "2.12.6",
        version := "0.1.0-SNAPSHOT"
      )),
    name := "fs2-kafka-streams",
    autoCompilerPlugins := true,
    addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.4"),
    addCompilerPlugin("com.lihaoyi"    %% "acyclic"        % "0.1.7"),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.2.4"),
    scalafmtOnCompile := true,
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients"             % "2.0.0",
      "org.typelevel"    %% "cats-core"                % "1.1.0",
      "org.typelevel"    %% "cats-effect"              % "1.0.0",
      "co.fs2"           %% "fs2-core"                 % "1.0.0-M5",
      "org.scodec"       %% "scodec-bits"              % "1.1.5",
      "org.scodec"       %% "scodec-core"              % "1.10.3",
      "org.rocksdb"      % "rocksdbjni"                % "5.13.2",
      "com.lihaoyi"      %% "acyclic"                  % "0.1.7" % "provided",
      "org.scalatest"    %% "scalatest"                % "3.0.4" % "it,test",
      "org.scalacheck"   %% "scalacheck"               % "1.14.0" % "it,test"
    ),
    scalacOptions ++= Seq("-P:acyclic:force")
  )
