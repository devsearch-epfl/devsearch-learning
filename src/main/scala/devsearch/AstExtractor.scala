package devsearch

import devsearch.parsers._
import devsearch.features._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import scala.util.parsing.combinator._


/**
 * Created by hubi on 3/27/15.
 */
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

//class PythonFile(size: Long, owner: String, repository: String, path: String, code: String)
//  extends CodeFile(size, owner, repository, path, code, PythonParser)

class GoFile(size: Long, owner: String, repository: String, path: String, code: String)
  extends CodeFile(size, owner, repository, path, code, GoParser)

//class JavaScriptFile(size: Long, owner: String, repository: String, path: String, code: String)
//  extends CodeFile(size, owner, repository, path, code, JavaScriptParser)

class ScalaFile(size: Long, owner: String, repository: String, path: String, code: String)
  extends CodeFile(size, owner, repository, path, code, QueryParser)

//case object UnknownFile() extends CodeFile


object SnippetParser extends RegexParsers with java.io.Serializable {

  def parseBlob: Parser[CodeFile] = (
    number~":../data/crawld/java/"~noSlash~"/"~noSlash~"/"~path~code ^^ {
      case size~_~owner~_~repo~_~path~code => new JavaFile(size.replace("\n", "").toLong, owner, repo, path, code)
    }
    //|number~":Python/"~noSlash~"/"~noSlash~"/"~path~code ^^ {
    //  case size~_~owner~_~repo~_~path~code => PythonFile(size, owner, repo, path, code)
    //}
    |number~":Go/"~noSlash~"/"~noSlash~"/"~path~code ^^ {
      case size~_~owner~_~repo~_~path~code => new GoFile(size.toLong, owner, repo, path, code)
    }
    |number~":Scala/"~noSlash~"/"~noSlash~"/"~path~code ^^ {
      case size~_~owner~_~repo~_~path~code => new ScalaFile(size.toLong, owner, repo, path, code)
    }
    //|number~":JavaScript/"~noSlash~"/"~noSlash~"/"~path~code ^^ {
    //  case size~_~owner~_~repo~_~path~code => JavaScriptFile(size, owner, repo, path, code)
    //}
  )

  val number:  Parser[String] = """[\n]?\d+""".r
  val noSlash: Parser[String] = """[^/]+""".r
  val path:    Parser[String] = """[^\n]+""".r                 //everything until eol
  val code:    Parser[String] = """(?s).*""".r                 //ethe rest
}




/* REPL helpers...


  def matchString(s: String, r: scala.util.matching.Regex): Boolean = s match{
    case r() => true
    case _   => false
  }


def showMatches(s: String, r: scala.util.matching.Regex): Unit = {
     for (m <- r.findAllIn(s)) println (m+"\n-------------------------------------------------")
}
 */





object AstExtractor {

  /**
   * A line is a header if...
   *  - there is exactly one ':'
   *  - there are only digits on the left side of the semicolon
   *  - if there are more than 7 slashes
   * @param s
   * @return Boolean
   */
  def matchHeader(s: String): Boolean = {
    val splitted = s.split(":")
    splitted.size == 2 && splitted(0).forall(_.isDigit) && (splitted(1).split("/").length >= 7)
  }


  /*def toBlobSnippet(blob: (String, String)): List[String] = {
    val snippet = """(?s).+?(?=(\n\d+:([a-zA-Z0-9\.]+/)|\Z))""".r    //match everything until some "<NUMBER>:" or end of string
    blob match {
      case (path, content) => snippet.findAllIn(content).toList
      case _               => List()
    }
  }*/


  /**
   * takes a BLOBsnippet and transforms it into a CodeFile
   * @param snippet
   * @return CodeFile
   */
  def toCodeFile(snippet: String): Option[CodeFile] = {
    val result = SnippetParser.parse(SnippetParser.parseBlob, snippet)
    if (result.isEmpty) None else Some(result.get)
  }


  def binarySearch(lineNumber : Long, headerLines : Array[(String, Long)]) = {
    def rec(lb : Int, ub :Int) : String = {
      val mb = (lb + ub) /2
      if(lb + 1 == ub) headerLines(lb)._1
      else if(headerLines(mb)._2 >lineNumber) rec(lb, mb)
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
    val fileHeaders = indexedLines.filter{case (line, _) => matchHeader(line)}.collect()


    //put all lines between two header lines into the same group
    val groupedLines = indexedLines.map{case (line, number) => ( binarySearch(number, fileHeaders),(line, number))}.groupBy(_._1)


    //put each group of lines together to a BLOBsnippet (format: <size>:../data/crawld/<language>/<owner>/<repo>/<path>\n<code>
    val snippets = groupedLines.map(list => list._2.foldLeft(""){(acc, line) => acc ++ (line._2._1 + "\n")})


    val codeFiles = snippets flatMap toCodeFile


    codeFiles
  }
}
