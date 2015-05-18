package devsearch.spark

import devsearch.ast.{ContentsSource, Empty, AST}
import devsearch.features._
import devsearch.parsers.Languages
import org.apache.hadoop.io.Text
import org.apache.spark.rdd._
import scala.util.parsing.combinator._

case class CodeFileMetadata(majorLanguage: String, location: CodeFileLocation) extends java.io.Serializable

object HeaderParser extends RegexParsers with java.io.Serializable {
  val noSlashRegex: Parser[String] = """[^/]+""".r
  val pathRegex: Parser[String] = """[^\n]+""".r

  def parseBlobHeader: Parser[CodeFileMetadata] = {
    noSlashRegex ~ "/" ~ noSlashRegex ~ "/" ~ noSlashRegex ~ "/" ~ pathRegex ^^ {
      case majorLanguage ~ _ ~ owner ~ _ ~ repoName ~ _ ~ fileName =>
        CodeFileMetadata(majorLanguage, CodeFileLocation(owner, repoName, fileName))
    }
  }
}

object AstExtractor {
  def extract(files: RDD[(Text, Text)]): RDD[CodeFile] = {
    files.flatMap { case (headerLine, content) =>
      val result = HeaderParser.parse(HeaderParser.parseBlobHeader, headerLine.toString)
      result match {
        case HeaderParser.Success(metadata, n) =>
          // Guess language (ignoring major language)
          Languages.guess(metadata.location.fileName) match {
            case Some(language) =>
              val uniqueOutputLocation = metadata.location.toString
              try {
                Some(CodeFile(
                  language, metadata.location,
                  new ContentsSource(uniqueOutputLocation, content.toString)
                ))
              } catch {
                case e: OutOfMemoryError => throw new OutOfMemoryError("++++++Error here: " + metadata.location.toString)
              }
            case None => None
          }
        case _ => None
      }
    }.filter(codeFile =>
      codeFile.ast != Empty[AST]
    )
  }
}
