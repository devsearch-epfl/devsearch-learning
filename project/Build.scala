import sbt._

object DevSearchLearning extends Build {

  lazy val root = Project("root", file(".")).dependsOn(astParser % "compile->compile;test->test")

  lazy val astCommit = "4835466aa21e11675f5b37442f403122e8a97312"
  lazy val astParser = RootProject(uri(s"git://github.com/devsearch-epfl/devsearch-ast.git#$astCommit"))
}
