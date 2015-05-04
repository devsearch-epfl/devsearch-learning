package devsearch.spark

import org.scalatest.FlatSpec

class BlobReaderTest extends FlatSpec {
  "The hadoop input format" should "retrieve the correct code from the blob" in {
    val headerSnippetPairs = Utility.retrieveSparkRdd()
    val keyValueList = headerSnippetPairs.map { case (key, value) => (key.toString, value.toString) }.collect().toList
    //println(keyValueList.map(_._1).mkString("\n"))
    // TODO(pwalch) Assert that the list contains some keys for the archive provided by Damien
  }
}
