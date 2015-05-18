package devsearch.stats

import devsearch.parsers.Languages
import devsearch.spark.BlobReader
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, TaskAttemptContext}


/**
 * This input format only works if files inside tarballs can be kept in memory
 */
class BlobHeaderInputFormat extends FileInputFormat[Text, Text] {
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): BlobHeaderReader = new BlobHeaderReader

  override def isSplitable(context: JobContext, filename: Path): Boolean = false
}

class BlobHeaderReader extends BlobReader {
  override def nextKeyValue(): Boolean = {
    val entry = tarInput.getNextEntry
    if (entry == null) {
      processed = true
    } else {
      val fileName = entry.getName
      if (Languages.isFileSupported(fileName)) {
        key.set(fileName)
        currentBlobSnippet.set("")
      }
    }

    val hasPairBeenFound = !processed
    hasPairBeenFound
  }
}