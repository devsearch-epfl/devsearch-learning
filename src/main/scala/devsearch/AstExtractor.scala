package devsearch

import devsearch.ast._
import devsearch.parsers._
import devsearch.features._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
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
    number~":Java/"~noSlash~"/"~noSlash~"/"~path~code ^^ {
      case size~_~owner~_~repo~_~path~code => new JavaFile(size.toLong, owner, repo, path, code)
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

  val number:  Parser[String] = """\d+""".r
  val noSlash: Parser[String] = """[^/]+""".r
  val path:    Parser[String] = """[^\n]+""".r                 //everything until eol
  val code:    Parser[String] = """(?s).*[^\n\d+:]""".r        //everything until "\n897162346:"
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

  def toBlobSnippet(blob: (String, String)): List[String] = {
    val (path, content) = blob
    val lines = content.split('\n')
    var ret = List[String]()

    var size = 0
    var snippet = ""
    for(line <- lines) {
      if (size <= 0){

        //add the snippet to the list...
        if(snippet != ""){
          ret :+= snippet
        }

        size = line.split(':')(0).toInt
        snippet = line
      } else {
        snippet += ("\n" + line)
        size -= (line.length + 1)     //chars on line + newline
      }
    }

    //add the last snippet
    ret :+= snippet

    ret
  }

  /*def toBlobSnippet(blob: (String, String)): List[String] = {
    val snippet = """(?s).+?(?=(\n\d+:|\Z))""".r    //match everything until some "<NUMBER>:" or end of string
    blob match {
      case (path, content) => snippet.findAllIn(content).toList
      case _               => List()
    }
  }*/

  def toCodeFile(snippet: String): Option[CodeFile] = {
    val result = SnippetParser.parse(SnippetParser.parseBlob, snippet)
    if (result.isEmpty) None else Some(result.get)
  }

  /*
   * Argument: a path that leads to the language directories
   */
  def extract(path: String)(implicit sc: SparkContext): RDD[CodeFile] = {
    // type: RDD(path: String, file: String)
    val rddBlobs = sc.wholeTextFiles(path+"/*")

    //TODO: check if path is valid!
    //TODO: uncompress files
    rddBlobs flatMap toBlobSnippet flatMap toCodeFile
  }
}
