import AssemblyKeys._  // put this at the top of the file

assemblySettings

target in Compile in doc := baseDirectory.value / "api"

name := "devsearch-learning"

shellPrompt := { state => "[\033[36m" + name.value + "\033[0m] $ " }

version := "0.1"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.1.7" % "test",
  "org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
  "org.apache.commons" % "commons-compress" % "1.9",
  "io.spray" %%  "spray-json" % "1.3.1"
)

parallelExecution in Test := false
