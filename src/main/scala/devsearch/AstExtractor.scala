package devsearch

import devsearch.ast.Empty.NoDef
import devsearch.features._
import org.apache.hadoop.io.Text
import org.apache.spark.rdd._
import scala.util.parsing.combinator._

case class CodeFileMetadata(size: Long, language: String, codeFileLocation: CodeFileLocation) extends java.io.Serializable
case class NoAstCodeFile(metadata: CodeFileMetadata, content: Text) extends java.io.Serializable

object HeaderParser extends RegexParsers with java.io.Serializable {
  val number: Parser[String] = """[\n]?\d+""".r
  val noSlashRegex: Parser[String] = """[^/]+""".r
  val pathRegex: Parser[String] = """[^\n]+""".r

  def parseBlob: Parser[CodeFileMetadata] = (
    number ~ ":../data/crawld/go/" ~ noSlashRegex ~ "/" ~ noSlashRegex ~ "/" ~ pathRegex ^^ {
      case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path =>
        CodeFileMetadata(size.replace("\n", "").toLong, "Go", CodeFileLocation(owner, repo, path))
    }
    | number ~ ":../data/crawld/java/" ~ noSlashRegex ~ "/" ~ noSlashRegex ~ "/" ~ pathRegex ^^ {
      case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path =>
        CodeFileMetadata(size.replace("\n", "").toLong, "Java", CodeFileLocation(owner, repo, path))
    }
    | number ~ ":../data/crawld/javascript/" ~ noSlashRegex ~ "/" ~ noSlashRegex ~ "/" ~ pathRegex ^^ {
      case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path =>
        CodeFileMetadata(size.replace("\n", "").toLong, "JavaScript", CodeFileLocation(owner, repo, path))
    }
    | number ~ ":../data/crawld/scala/" ~ noSlashRegex ~ "/" ~ noSlashRegex ~ "/" ~ pathRegex ^^ {
      case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path =>
        CodeFileMetadata(size.replace("\n", "").toLong, "Scala", CodeFileLocation(owner, repo, path))
    }
  )
}

object AstExtractor {
  val MEGABYTES = Math.pow(2, 20)

  def extract(files: RDD[(Text, Text)]): RDD[CodeFileData] = {
    files.flatMap { case (headerLine, content) =>
      val result = HeaderParser.parse(HeaderParser.parseBlob, headerLine.toString)
      if (result.isEmpty) None
      else Some(NoAstCodeFile(result.get, content))
    }.filter { case NoAstCodeFile(metadata, content) =>
      metadata.size <= 10 * MEGABYTES
    }.map { case NoAstCodeFile(metadata: CodeFileMetadata, content: Text) =>
      CodeFileData(metadata.size, metadata.language, metadata.codeFileLocation, content.toString)
    }.filter(codeFile =>
      codeFile.ast != NoDef
    )
  }
}
