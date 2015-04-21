package devsearch

import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FileInputFormat}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{TaskAttemptContext, InputSplit, RecordReader}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.commons.io.IOUtils
import java.io.{BufferedReader, InputStreamReader}


class BlobInputFormat extends FileInputFormat[Text, Text] {
  def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Text, Text] = new BlobReader
}

/**
 * The BlobReader goes through a BLOB line by line and returns all contained BLOBsnippets.
 */
class BlobReader extends RecordReader[Text, Text] {
  var key = new Text("")
  var value = new Text("")
  var processed = false
  var currHeader = ""
  var currFile = ""

  var bufferedReader: BufferedReader = _

  private def matchHeader(s: String): Boolean = {
    val splitted = s.split(":")
    splitted.size == 2 && splitted(0).forall(_.isDigit) && (splitted(1).split("/").length >= 7)
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
      currFile = ""
      Stream.continually(bufferedReader.readLine())
          .takeWhile(l => l != null && !matchHeader(l)) //take all the lines until the next header or eof
          .foreach((l: String) => currFile += l) //concat the lines together

      if (currFile.isEmpty) {
        processed = true
        return false
      }

      key.set(new Text("bla"))
      value.set(new Text(currFile))

    } catch {
      case e: Exception => e.printStackTrace
    }

    return true
  }

  def getCurrentKey: Text = key

  def getCurrentValue: Text = value

  def getProgress: Float = if (processed) 1.0f else 0.0f
}