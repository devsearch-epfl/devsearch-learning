package devsearch.spark

import org.scalatest.FlatSpec

class BlobReaderTest extends FlatSpec {
  "The hadoop input format" should "retrieve the correct code from the blob" in {
    val headerSet = Utility.headerSnippetPairs.map(_._1.toString).collect().toSet

    assert(
      Set[String](
        "java/typesafehub/config/project/Build.scala"
      ).subsetOf(headerSet)
    )
  }
}
