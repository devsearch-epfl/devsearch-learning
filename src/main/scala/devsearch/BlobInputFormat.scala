package devsearch

/**
 * Created by hubi on 4/17/15.
 */
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FileInputFormat};
import org.apache.hadoop.io.{Text}
import org.apache.hadoop.mapreduce.{TaskAttemptContext, InputSplit, RecordReader}
import org.apache.hadoop.fs.{Path, FileSystem, FSDataInputStream}
import org.apache.commons.io.IOUtils
import java.io.{BufferedReader, InputStreamReader}





class BlobInputFormat extends FileInputFormat[Text, Text]{
  println("\n\n\n\n\n\n\n\nCreated BlobInputFormat\n\n\n\n\n\n\n\n")

  def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Text, Text] = new BlobReader
}





/**
 * The BlobReader goes through a BLOB line by line and returns all contained BLOBsnippets.
 */
class BlobReader extends RecordReader[Text, Text]{
  println("\n\n\n\n\n\n\n\nInit BlobReader...\n\n\n\n\n\n\n\n")
  var key   = new Text("")
  var value = new Text("")
  var processed = false
  var currHeader = ""
  var currFile   = ""

  var fileSplit:  FileSplit  = _
  var fileSystem: FileSystem = _
  var path:       Path       = _

  var in: FSDataInputStream = _
  var ir: InputStreamReader = _
  var br: BufferedReader    = _


  private def matchHeader(s: String): Boolean = {
    val splitted = s.split(":")
    splitted.size == 2 && splitted(0).forall(_.isDigit) && (splitted(1).split("/").length >= 7)
  }


  def close(): Unit = {
    IOUtils.closeQuietly(br)
    IOUtils.closeQuietly(ir)
    IOUtils.closeQuietly(in)
  }


  def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    println("\n\n\n\n\n\n\n\nInit BlobReader...\n\n\n\n\n\n\n\n")
    fileSplit  = split.asInstanceOf[FileSplit]
    path       = fileSplit.getPath
    fileSystem = path.getFileSystem(context.getConfiguration)
  }


  /**
   * creates the next key-value pair
   */
  def nextKeyValue(): Boolean = {
    try {
      in = fileSystem.open(path)
      ir = new InputStreamReader(in)
      br = new BufferedReader(ir)


      currFile = ""

      Stream.continually(br.readLine())
            .takeWhile(l => l != null && !matchHeader(l))     //take all the lines until the next header or eof
            .foreach((l: String) => currFile += l)           //concat the lines together

      if(currFile.isEmpty) {
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


  def getProgress: Float = if(processed) 1.0f else 0.0f
}