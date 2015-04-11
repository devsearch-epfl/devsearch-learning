import sbt._

object DevSearchLearning extends Build {

  lazy val root = Project("root", file(".")).dependsOn(astParser % "compile->compile;test->test")

  lazy val astCommit = "55a86c047a14526d856bf197fda7112d1ff9a470"
  lazy val astParser = RootProject(uri(s"git://github.com/devsearch-epfl/devsearch-ast.git#$astCommit"))
  // lazy val astParser = RootProject(file("../devsearch-ast"))
}
