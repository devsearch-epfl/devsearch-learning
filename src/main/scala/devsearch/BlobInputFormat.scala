package devsearch

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FileInputFormat}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext, InputSplit, RecordReader}
import org.apache.commons.io.IOUtils
import java.io.{BufferedReader, InputStreamReader}


class BlobInputFormat extends FileInputFormat[Text, Text] {
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): BlobReader =
    new BlobReader

  override def isSplitable(context: JobContext, filename: Path): Boolean = false
}

/**
 * The BlobReader goes through a BLOB line by line and returns all contained BLOBsnippets.
 */
class BlobReader extends RecordReader[Text, Text] {
  var key = new Text("")
  var currentBlobSnippet = new Text("")
  var processed = false

  var bufferedReader: BufferedReader = _

  private def matchHeader(s: String): Boolean = {
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

  override def close(): Unit = {
    IOUtils.closeQuietly(bufferedReader)
  }

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val firstSplit = split.asInstanceOf[FileSplit]
    val path = firstSplit.getPath
    val fileSystem = path.getFileSystem(context.getConfiguration())

    println("Init BlobReader at path: " + path)

    //TODO: Strange error here! It looks like the two hadoop APIs would be mixed. But i don't do this... :-/
    bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
  }

  /**
   * creates the next key-value pair
   */
  override def nextKeyValue(): Boolean = {
    try {
      val header = bufferedReader.readLine()

      // If we don't have any header, then we reached the end of the blob
      if (header == Nil) {
        processed = true
        return false
      }

      var lineAcc = "" + header + "\n"

      // If we have a header, we take the content of this snippet
      Stream.continually(bufferedReader.readLine())
          // Take all the lines until the next header (excluded) or EOF
          .takeWhile(l => l != null && !matchHeader(l))
          // Add line to accumulator
          .foreach(l => lineAcc += l + "\n")

      key.set(new Text("bla"))
      currentBlobSnippet.set(new Text(lineAcc))

      lineAcc = ""
      return true
    } catch {
      case e: Exception => e.printStackTrace
    }

    return false
  }

  override def getCurrentKey: Text = key

  override def getCurrentValue: Text = currentBlobSnippet

  override def getProgress: Float = if (processed) 1.0f else 0.0f
}