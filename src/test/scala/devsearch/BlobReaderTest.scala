package devsearch

import org.scalatest.FlatSpec

class BlobReaderTest extends FlatSpec {
  "HeaderMatches" should "match a good header" in {
    assert(HeaderMatcher.isMatching("13967:../data/crawld/java/elasticsearch/elasticsearch/src/main/java/org/elasticsearch/common/rounding/TimeZoneRounding.java"))
  }

  it should "not match a bad header" in {
    assert(!HeaderMatcher.isMatching("13967:../data/crawld/java/ NOT A HEADER"))
  }

  it should "match a good header 2" in {
    assert(HeaderMatcher.isMatching("13967:../data/crawld/java/elasticsearch/elasticsearch/README.md"))
  }
}