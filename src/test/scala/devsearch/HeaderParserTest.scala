package devsearch

import devsearch.features.CodeFileLocation
import devsearch.parsers.Languages
import org.scalatest.FlatSpec

class HeaderParserTest extends FlatSpec {
  "Header parser" should "parse correct string" in {
    val header = "15466:../data/crawld/java/acrobot/chestshop-3/src/main/java/com/lennardf1989/bukkitex/Database.java"
    val result = HeaderParser.parse(HeaderParser.parseBlobHeader, header)

    val headerResult = {
      if (result.isEmpty) None
      else result.get
    }

    assert(
      headerResult ==
      CodeFileMetadata(
        15466,
        Languages.Java,
        CodeFileLocation("acrobot", "chestshop-3",
          "src/main/java/com/lennardf1989/bukkitex/Database.java"
        )
      )
    )
  }
}
