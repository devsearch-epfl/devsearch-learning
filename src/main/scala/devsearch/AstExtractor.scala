package devsearch

import devsearch.ast.Empty.NoDef
import devsearch.features._
import org.apache.hadoop.io.Text
import org.apache.spark.rdd._
import scala.util.parsing.combinator._

case class NoAstCodeFile(size: Long, language: String, codeFileLocation: CodeFileLocation, code: String) extends java.io.Serializable

object SnippetParser extends RegexParsers with java.io.Serializable {
  val number: Parser[String] = """[\n]?\d+""".r
  val noSlash: Parser[String] = """[^/]+""".r
  val path: Parser[String] = """[^\n]+""".r
  // Everything until end of line
  val code: Parser[String] = """(?s).*""".r // the rest

  def parseBlob: Parser[NoAstCodeFile] = (
    number ~ ":../data/crawld/java/" ~ noSlash ~ "/" ~ noSlash ~ "/" ~ path ~ code ^^ {
        case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path ~ code =>
          NoAstCodeFile(size.replace("\n", "").toLong, "Java", CodeFileLocation(owner, repo, path), code)
    }
    | number ~ ":Go/" ~ noSlash ~ "/" ~ noSlash ~ "/" ~ path ~ code ^^ {
        case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path ~ code =>
          NoAstCodeFile(size.toLong, "Go", CodeFileLocation(owner, repo, path), code)
    }
    | number ~ ":Scala/" ~ noSlash ~ "/" ~ noSlash ~ "/" ~ path ~ code ^^ {
        case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path ~ code =>
          NoAstCodeFile(size.toLong, "Scala", CodeFileLocation(owner, repo, path), code)
    }
  )
}

object AstExtractor {
  val MEGABYTES = Math.pow(2, 20)

  def extract(files: RDD[(Text, Text)]): RDD[CodeFileData] = {
    files.map {
      case (headerLine, content) => headerLine + "\n" + content
    }.flatMap { snippet =>
      val result = SnippetParser.parse(SnippetParser.parseBlob, snippet)
      if (result.isEmpty) None
      else Some(result.get)
    }.filter { case NoAstCodeFile(size, _, _, _) =>
      size <= 10 * MEGABYTES
    }.map { case NoAstCodeFile(size, language, codeFileLocation, code) =>
        CodeFileData(size, language, codeFileLocation, code)
    }.filter(c =>
      c.ast != NoDef
    )
  }
}
