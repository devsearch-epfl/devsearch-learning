package devsearch.stats

import devsearch.spark.{HeaderParser, FeatureMining}
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object StatMining {
  def readInput(sc: SparkContext, files: List[String]): RDD[(Text, Text)] = {
    sc.union(
      files.map(path =>
        sc.newAPIHadoopFile(path, classOf[BlobHeaderInputFormat], classOf[Text], classOf[Text])
      )
    )
  }

  def main(args: Array[String]) {
    if (args.length != 3) {
      println("Invalid arguments. Args are: input_dir output_dir job_name")
      return
    }
    // val javaInputDir = "hdfs:///projects/devsearch/pwalch/usable_repos/java"
    // val javaOutputDir = "hdfs:///projects/devsearch/pwalch/features/java"
    // val javaJobName = "devsearch_JavaFeatureMining"

    val inputDir = args(0)
    val outputDir = args(1)
    val jobName = args(2)
    val inputDirNoSlash = inputDir.replaceAll("/$", "")

    val sc = FeatureMining.getSparkContext(jobName)
    val fileList = FeatureMining.getFileList(inputDirNoSlash)
    val blobs: RDD[(Text, Text)] = readInput(sc, fileList)
    val locations = blobs.keys.flatMap { headerLine =>
      val result = HeaderParser.parse(HeaderParser.parseBlobHeader, headerLine.toString)
      result match {
        case HeaderParser.Success(metadata, n) =>
          List(metadata.location.toString)

        case _ =>
          List()
      }
    }

    locations.saveAsTextFile(outputDir)

    println("Mining done")
  }
}
