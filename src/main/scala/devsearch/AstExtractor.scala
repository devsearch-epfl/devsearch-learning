package devsearch

import devsearch.ast.{Empty, AST}
import devsearch.features._
import devsearch.parsers.Languages
import org.apache.hadoop.io.Text
import org.apache.spark.rdd._
import scala.util.parsing.combinator._

case class CodeFileMetadata(size: Long, majorLanguage: String, location: CodeFileLocation) extends java.io.Serializable

object HeaderParser extends RegexParsers with java.io.Serializable {
  val numberRegex: Parser[String] = """[\n]?\d+""".r
  val noSlashRegex: Parser[String] = """[^/]+""".r
  val pathRegex: Parser[String] = """[^\n]+""".r

  def parseBlobHeader: Parser[CodeFileMetadata] = (
      numberRegex ~ ":../data/crawld/go/" ~ noSlashRegex ~ "/" ~ noSlashRegex ~ "/" ~ pathRegex ^^ {
        case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path =>
          CodeFileMetadata(size.replace("\n", "").toLong, Languages.Go, CodeFileLocation(owner, repo, path))
      }
      | numberRegex ~ ":../data/crawld/java/" ~ noSlashRegex ~ "/" ~ noSlashRegex ~ "/" ~ pathRegex ^^ {
        case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path =>
          CodeFileMetadata(size.replace("\n", "").toLong, Languages.Java, CodeFileLocation(owner, repo, path))
      }
      | numberRegex ~ ":../data/crawld/javascript/" ~ noSlashRegex ~ "/" ~ noSlashRegex ~ "/" ~ pathRegex ^^ {
        case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path =>
          CodeFileMetadata(size.replace("\n", "").toLong, Languages.JavaScript, CodeFileLocation(owner, repo, path))
      }
      | numberRegex ~ ":../data/crawld/scala/" ~ noSlashRegex ~ "/" ~ noSlashRegex ~ "/" ~ pathRegex ^^ {
        case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path =>
          CodeFileMetadata(size.replace("\n", "").toLong, Languages.Scala, CodeFileLocation(owner, repo, path))
      }
  )
}

object AstExtractor {
  def extract(files: RDD[(Text, Text)]): RDD[CodeFileData] = {
    files.flatMap { case (headerLine, content) =>
      val result = HeaderParser.parse(HeaderParser.parseBlobHeader, headerLine.toString)
      if (result.isEmpty) {
        None
      } else {
        val metadata = result.get

        // Guess language (ignoring major language)
        Languages.guess(metadata.location.fileName) match {
          case Some(language) => Some(CodeFileData(metadata.size, language, metadata.location, content.toString))
          case None => None
        }
      }
    }.filter(codeFile =>
      codeFile.ast != Empty[AST]
    )
  }
}
