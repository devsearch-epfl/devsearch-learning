package devsearch.spark

import devsearch.features.{Feature, FeatureRecognizer}
import org.scalatest.FlatSpec

class MiningTest extends FlatSpec {
  "Feature mining" should "extract some features" in {
    val featureSet = AstExtractor.extract(Utility.headerSnippetPairs).flatMap(FeatureRecognizer).collect.toSet
    assert(Set[Feature]().subsetOf(featureSet))
  }
}
