package devsearch

import devsearch.features._
import org.apache.hadoop.io.Text
import org.apache.spark.rdd._
import scala.util.parsing.combinator._

object SnippetParser extends RegexParsers with java.io.Serializable {
  val number: Parser[String] = """[\n]?\d+""".r
  val noSlash: Parser[String] = """[^/]+""".r
  val path: Parser[String] = """[^\n]+""".r
  // Everything until end of line
  val code: Parser[String] = """(?s).*""".r // the rest

  def parseBlob: Parser[CodeFileData] = (
    number ~ ":../data/crawld/java/" ~ noSlash ~ "/" ~ noSlash ~ "/" ~ path ~ code ^^ {
        case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path ~ code =>
          CodeFileData(size.replace("\n", "").toLong, "Java", CodeFileLocation(owner, repo, path), code)
    }
    | number ~ ":Go/" ~ noSlash ~ "/" ~ noSlash ~ "/" ~ path ~ code ^^ {
        case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path ~ code =>
          CodeFileData(size.toLong, "Go", CodeFileLocation(owner, repo, path), code)
    }
    | number ~ ":Scala/" ~ noSlash ~ "/" ~ noSlash ~ "/" ~ path ~ code ^^ {
        case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path ~ code =>
          CodeFileData(size.toLong, "Scala", CodeFileLocation(owner, repo, path), code)
    }
  )
}

object AstExtractor {
  def extract(files: RDD[(Text, Text)]): RDD[CodeFileData] = {
    files.map {
      case (headerLine, content) => headerLine + "\n" + content
    } flatMap {
      case snippet: String =>
        val result = SnippetParser.parse(SnippetParser.parseBlob, snippet)
        if (result.isEmpty) None
        else Some(result.get)
    }
  }
}
