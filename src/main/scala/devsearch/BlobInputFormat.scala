package devsearch

import devsearch.parsers.Languages
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FileInputFormat}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext, InputSplit, RecordReader}
import org.apache.commons.io.IOUtils
import java.io.{BufferedInputStream, BufferedReader, InputStreamReader}

import org.kamranzafar.jtar.TarInputStream


class BlobInputFormat extends FileInputFormat[Text, Text] {
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): BlobReader = new BlobReader

  override def isSplitable(context: JobContext, filename: Path): Boolean = false
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

  var tarInput: TarInputStream = _

  override def close(): Unit = {
    IOUtils.closeQuietly(tarInput)
  }

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val firstSplit = split.asInstanceOf[FileSplit]
    val path = firstSplit.getPath
    val fileSystem = path.getFileSystem(context.getConfiguration())

    tarInput = new TarInputStream(new BufferedInputStream(fileSystem.open(path)));
  }

  /**
   * creates the next key-value pair
   */
  override def nextKeyValue(): Boolean = {
    val entry = tarInput.getNextEntry
    if (entry == null) {
      processed = true
    } else {
      key.set(entry.getName)
      val bytes = new Array[Byte](entry.getSize.toInt)
      IOUtils.readFully(tarInput, bytes)
      currentBlobSnippet.set(bytes)
    }

    !processed
  }

  override def getCurrentKey: Text = key

  override def getCurrentValue: Text = currentBlobSnippet

  override def getProgress: Float = if (processed) 1.0f else 0.0f
}