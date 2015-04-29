import sbt._

object DevSearchLearning extends Build {

  lazy val root = Project("root", file(".")).dependsOn(astParser % "compile->compile;test->test")

  lazy val astCommit = "7ccfc86af419dba869362b7ca4021588ca1e80a9"
  lazy val astParser = RootProject(uri(s"git://github.com/devsearch-epfl/devsearch-ast.git#$astCommit"))
}
