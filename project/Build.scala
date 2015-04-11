import sbt._

object DevSearchLearning extends Build {

  lazy val root = Project("root", file(".")).dependsOn(astParser % "compile->compile;test->test")

  lazy val astCommit = "f2681ef71cbf68722825b7ab50c13cb4143f75a8"
  //lazy val astParser = RootProject(uri(s"git://github.com/devsearch-epfl/devsearch-ast.git#$astCommit"))
  lazy val astParser = RootProject(file("../devsearch-ast"))
}
