import AssemblyKeys._  // put this at the top of the file

assemblySettings



name := "devsearch-learning"

version := "0.1"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.1.7" % "test",
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"
)

parallelExecution in Test := false