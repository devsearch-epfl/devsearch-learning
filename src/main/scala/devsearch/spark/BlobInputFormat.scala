package devsearch.spark

import devsearch.parsers.Languages
import org.apache.commons.compress.archivers.{ArchiveStreamFactory, ArchiveInputStream}
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FileInputFormat}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext, InputSplit, RecordReader}
import org.apache.commons.io.IOUtils
import java.io.BufferedInputStream


/**
 * This input format only works if files inside tarballs can be kept in memory
 */
class BlobInputFormat extends FileInputFormat[Text, Text] {
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): BlobReader = new BlobReader

  override def isSplitable(context: JobContext, filename: Path): Boolean = false
}

class BlobReader extends RecordReader[Text, Text] {
  var key = new Text("")
  var currentBlobSnippet = new Text("")
  var processed = false

  var tarInput: ArchiveInputStream = _

  override def close(): Unit = {
    IOUtils.closeQuietly(tarInput)
  }

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val firstSplit = split.asInstanceOf[FileSplit]
    val path = firstSplit.getPath
    val fileSystem = path.getFileSystem(context.getConfiguration())

    tarInput = new ArchiveStreamFactory().createArchiveInputStream(ArchiveStreamFactory.TAR, new BufferedInputStream(fileSystem.open(path)));
  }

  override def nextKeyValue(): Boolean = {
    val entry = tarInput.getNextEntry
    if (entry == null) {
      processed = true
    } else {
      val fileName = entry.getName
      if (Languages.isFileSupported(fileName)) {
        key.set(fileName)
        val bytes = new Array[Byte](entry.getSize.toInt)
        IOUtils.readFully(tarInput, bytes)
        currentBlobSnippet.set(bytes)
      }
    }

    val hasPairBeenFound = !processed
    hasPairBeenFound
  }

  override def getCurrentKey: Text = key

  override def getCurrentValue: Text = currentBlobSnippet

  override def getProgress: Float = if (processed) 1.0f else 0.0f
}