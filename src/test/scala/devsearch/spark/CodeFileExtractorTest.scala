package devsearch.spark

import devsearch.features.CodeFileLocation
import org.scalatest.FlatSpec

class CodeFileExtractorTest extends FlatSpec {
  "AST extractor" should "extract some correct metadata" in {
    val codeFileLocationSet = AstExtractor.extract(Utility.headerSnippetPairs).map(_.location).collect.toSet

    assert(
      Set[CodeFileLocation](
        CodeFileLocation("typesafehub", "config", "SimpleLibContext.java")
      ).subsetOf(codeFileLocationSet)
    )
  }
}
