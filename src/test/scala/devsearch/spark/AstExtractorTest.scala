package devsearch.spark

import devsearch.features.CodeFileLocation
import org.scalatest.FlatSpec

class AstExtractorTest extends FlatSpec {
  "AST extractor" should "extract some correct metadata" in {
    val codeFileLocationSet = AstExtractor.extract(Utility.headerSnippetPairs).map(_.location).collect.toSet

    assert(
      Set[CodeFileLocation](
        CodeFileLocation("typesafehub", "config", "project/Build.scala")
      ).subsetOf(codeFileLocationSet)
    )
  }
}
