package devsearch

import java.io.File
import org.apache.hadoop.io.Text
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec}

class BlobReaderTest extends FlatSpec {
  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def absResourcePath(path: String): String = {
    val fileURL = getClass.getResource(path)
    new java.io.File(fileURL.toURI).getAbsolutePath
  }

  "The hadoop input format" should "retrieve the correct code from the blob" in {
    val conf = new SparkConf().setMaster("local[2]").setAppName("BlobReaderTest")
    val sc = new SparkContext(conf)

    val filenameList = recursiveListFiles(new File(absResourcePath("/concat"))).map(file => file.getAbsolutePath)
    val headerSnippetPairs = sc.union(
      filenameList.map(path =>
        sc.newAPIHadoopFile(path, classOf[BlobInputFormat], classOf[Text], classOf[Text])
      )
    )
    val keyValueList = headerSnippetPairs.map { case (key, value) => (key.toString, value.toString) }.collect().toList
    // TODO(pwalch) Assert that the list contains some keys for the archive provided by Damien
  }
}
