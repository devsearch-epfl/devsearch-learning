package devsearch

import devsearch.parsers.Languages
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FileInputFormat}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext, InputSplit, RecordReader}
import org.apache.commons.io.{FilenameUtils, IOUtils}
import java.io.{BufferedReader, InputStreamReader}


class BlobInputFormat extends FileInputFormat[Text, Text] {
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): BlobReader =
    new BlobReader

  override def isSplitable(context: JobContext, filename: Path): Boolean = false
}

object HeaderMatcher {
  def isMatching(s: String): Boolean = {
    val splittedLine = s.split(":")
    if (splittedLine.length != 2) {
      return false
    }

    val fileLength = splittedLine(0)
    val headerRest = splittedLine(1)
    val splittedRest = headerRest.split("/")

    if (splittedRest.length <= 6) {
      return false
    }

    return splittedRest(0) == ".." && splittedRest(1) == "data" && splittedRest(2) == "crawld" &&
        fileLength.forall(_.isDigit)
  }
}

/**
 * The BlobReader goes through a BLOB line by line and returns all contained BLOBsnippets.
 */
class BlobReader extends RecordReader[Text, Text] {
  val MEGABYTES = Math.pow(2, 20)
  val MAX_FILE_SIZE = 2 * MEGABYTES

  var key = new Text("")
  var currentBlobSnippet = new Text("")
  var processed = false
  var lastLineRead = ""

  var bufferedReader: BufferedReader = _

  override def close(): Unit = {
    IOUtils.closeQuietly(bufferedReader)
  }

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val firstSplit = split.asInstanceOf[FileSplit]
    val path = firstSplit.getPath
    val fileSystem = path.getFileSystem(context.getConfiguration())

    bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
  }

  /**
   * creates the next key-value pair
   */
  override def nextKeyValue(): Boolean = {
    val header = lastLineRead match {
      // If very first iteration
      case "" => bufferedReader.readLine()
      // If EOF has not been reached
      case headerLine: String => headerLine
      // If end of file
      case _ => Nil
    }

    // If we don't have any header, then we reached the end of the blob
    if (header == Nil) {
      processed = true
      return false
    }

    // We verify that we need to parse the following file
    val parsedHeader = HeaderParser.parse(HeaderParser.parseBlobHeader, header.toString)
    var needToParse = true
    if (!parsedHeader.isEmpty) {
      val metadata = parsedHeader.get
      val extensionMatchesLanguage = Languages.extension(metadata.language) ==
          FilenameUtils.getExtension(metadata.codeFileLocation.fileName)

      if (metadata.size >= MAX_FILE_SIZE || !extensionMatchesLanguage) {
        needToParse = false
      }
    }

    // If we have a header, we take the content of this snippet
    var lineAcc = ""
    Stream.continually {
      lastLineRead = bufferedReader.readLine()
      lastLineRead
    }
    // Take all the lines until the next header (excluded) or EOF
    .takeWhile(l => l != null && !HeaderMatcher.isMatching(l))
    .foreach(l => if(needToParse) lineAcc += l + "\n")

    key.set(new Text(header.toString))
    currentBlobSnippet.set(new Text(lineAcc))

    true
  }

  override def getCurrentKey: Text = key

  override def getCurrentValue: Text = currentBlobSnippet

  override def getProgress: Float = if (processed) 1.0f else 0.0f
}