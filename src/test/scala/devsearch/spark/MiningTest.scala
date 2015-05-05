package devsearch.spark

import devsearch.features._
import org.scalatest.FlatSpec

class MiningTest extends FlatSpec {
  "Feature mining" should "extract some features" in {
    val featureSet = AstExtractor.extract(Utility.headerSnippetPairs).flatMap(FeatureRecognizer).collect.toSet
    assert(
      Set[Feature](
        TypeRefFeature(
          CodePiecePosition(CodeFileLocation("fwbrasil", "activate", "SlickQueryContext.scala"), 42),
          "JdbcRelationalStorage"
        )
      ).subsetOf(featureSet)
    )
  }
}
