import sbt._

object DevSearchLearning extends Build {

  lazy val root = Project("root", file(".")).dependsOn(astParser % "compile->compile;test->test")

  lazy val astCommit = "af4a968fa551b186f4c4f5eb6f9db60432da2826"
  lazy val astParser = RootProject(uri(s"git://github.com/devsearch-epfl/devsearch-ast.git#$astCommit"))
}
