package devsearch

import devsearch.parsers._
import devsearch.features._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import scala.util.parsing.combinator._

abstract class CodeFile(val size: Long, val language: String, val data: CodeFileData) extends java.io.Serializable {
  def this(size: Long, owner: String, repository: String, path: String, code: String, parser: Parser) =
    this(size, parser.language, CodeFileData(CodeFileLocation(owner, repository, path), parser, code))

  override def equals(that: Any): Boolean = that match {
    case cf: CodeFile => size == cf.size && language == cf.language && data == cf.data
    case _ => false
  }

  override def hashCode: Int = size.hashCode + 16 * language.hashCode + 31 * data.hashCode
}

class JavaFile(size: Long, owner: String, repository: String, path: String, code: String)
    extends CodeFile(size, owner, repository, path, code, JavaParser)

class GoFile(size: Long, owner: String, repository: String, path: String, code: String)
    extends CodeFile(size, owner, repository, path, code, GoParser)

class ScalaFile(size: Long, owner: String, repository: String, path: String, code: String)
    extends CodeFile(size, owner, repository, path, code, QueryParser)


object SnippetParser extends RegexParsers with java.io.Serializable {

  def parseBlob: Parser[CodeFile] = (
    number ~ ":../data/crawld/java/" ~ noSlash ~ "/" ~ noSlash ~ "/" ~ path ~ code ^^ {
        case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path ~ code => new JavaFile(size.replace("\n", "").toLong, owner, repo, path, code)
    }
    | number ~ ":Go/" ~ noSlash ~ "/" ~ noSlash ~ "/" ~ path ~ code ^^ {
        case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path ~ code => new GoFile(size.toLong, owner, repo, path, code)
    }
    | number ~ ":Scala/" ~ noSlash ~ "/" ~ noSlash ~ "/" ~ path ~ code ^^ {
        case size ~ _ ~ owner ~ _ ~ repo ~ _ ~ path ~ code => new ScalaFile(size.toLong, owner, repo, path, code)
    }
  )

  val number: Parser[String] = """[\n]?\d+""".r
  val noSlash: Parser[String] = """[^/]+""".r
  val path: Parser[String] = """[^\n]+""".r
  // Everything until eol
  val code: Parser[String] = """(?s).*""".r // the rest
}

object AstExtractor {

  /**
   * A line is a header if...
   * - there is exactly one ':'
   * - there are only digits on the left side of the semicolon
   * - if there are more than 7 slashes
   */
  def matchHeader(s: String): Boolean = {
    val splitted = s.split(":")
    splitted.size == 2 && splitted(0).forall(_.isDigit) && (splitted(1).split("/").length >= 7)
  }

  /**
   * Takes a BLOBsnippet and transforms it into a CodeFile
   */
  def toCodeFile(snippet: String): Option[CodeFile] = {
    val result = SnippetParser.parse(SnippetParser.parseBlob, snippet)
    if (result.isEmpty) None else Some(result.get)
  }

  def binarySearch(lineNumber: Long, headerLines: Array[(String, Long)]) = {
    def rec(lb: Int, ub: Int): String = {
      val mb = (lb + ub) / 2
      if (lb + 1 == ub || lb == ub) headerLines(lb)._1
      else if (headerLines(mb)._2 > lineNumber) rec(lb, mb)
      else rec(mb, ub)
    }
    rec(0, headerLines.size)
  }

  /*
   * Argument: a path that leads to the language directories
   */
  def extract(path: String)(implicit sc: SparkContext): RDD[CodeFile] = {

    //save the BLOB line by line and index the lines
    val lines = sc.textFile(path)
    val indexedLines = lines.zipWithIndex()

    //identify and extract the first line of each BLOBsnippet. These headers must be collected because they are needed
    //in the binarySearch function together with an other RDD .
    val fileHeaders = indexedLines.filter { case (line, _) => matchHeader(line) }.collect()

    //put all lines between two header lines into the same group
    val groupedLines = indexedLines.map { case (line, number) => (binarySearch(number, fileHeaders), (line, number)) }.groupBy(_._1)

    //put each group of lines together to a BLOBsnippet (format: <size>:../data/crawld/<language>/<owner>/<repo>/<path>\n<code>
    val snippets = groupedLines.map(list => list._2.foldLeft("") { (acc, line) => acc ++ (line._2._1 + "\n") })

    //println("\n\n\n\n\n\n\n\nExtracted "+snippets.count()+ " snippets.\n\n\n\n\n\n\n\n")
    snippets flatMap toCodeFile
  }
}
