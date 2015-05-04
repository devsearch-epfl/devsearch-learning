package devsearch.spark

import java.io.File

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Utility {
  val conf = new SparkConf().setMaster("local[2]").setAppName("BlobReaderTest")
  val sc = new SparkContext(conf)

  val filenameList = Utility.listConcatFiles()
  val headerSnippetPairs = FeatureMining.readInput(sc, filenameList)

  private def listConcatFiles(): List[String] = {
    recursiveListFiles(new File(absResourcePath("/concat"))).map(file => file.getAbsolutePath).toList
  }

  private def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  private def absResourcePath(path: String): String = {
    val fileURL = getClass.getResource(path)
    new java.io.File(fileURL.toURI).getAbsolutePath
  }
}
