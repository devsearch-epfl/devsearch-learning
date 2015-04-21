package devsearch

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text

object FeatureMining {

  //TODO: SET PATH TO CORRECT REPOSITORY DIRECTORY HERE!
  val inputDir = "/Users/pierre/Documents/myfiles/EPFL/ProjetBigData/spark_input"
  val outputDir = "/Users/pierre/Documents/myfiles/EPFL/ProjetBigData/spark_output/my_output"

  /**
   * We need to process the BLOBs file by file because the header line of each BLOBsnippet gets collected in AstExtractor.
   * Since the BLOBs are so huge this would cause problems if we extracted all BLOBs in parallel.
   */
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FeatureMining").setMaster("local[2]")
    implicit val sc = new SparkContext(conf)

    // Go through each language directory and list all the contained BLOBs
    val fs = FileSystem.get(new java.net.URI(inputDir + "/*"), new Configuration())
    val blobPathList = fs.listStatus(new Path(inputDir))
        // Language directories
        .map(_.getPath)
        // Files in the language directories
        .flatMap(p => fs.listStatus(p))
        .map(_.getPath.toString)


    val keyValueAccumulator: RDD[(Text, Text)] = sc.emptyRDD[(Text, Text)]
    val fileRdd = blobPathList.foldLeft(keyValueAccumulator)((acc, path) =>
      sc.union(acc, sc.newAPIHadoopFile(path, classOf[BlobInputFormat], classOf[Text], classOf[Text]))
    )

    fileRdd.flatMap {
      case (key, value) => List(AstExtractor extrac)
      case _ => List()
    }

    //process each BLOB
    for (inputFile <- blobPathList) {
      println("Processing " + inputFile)
      println("Generated " + .count() + " snippets.")
      val codeFiles = AstExtractor extract inputFile
      val features = CodeEater eat codeFiles
      //println("\n\n\n\n\n\n\n\nGenerated "+features.count()+ " features from " + codeFiles.count + " files.\n\n\n\n\n\n\n\n")
      features map(_.toString) saveAsTextFile(outputDir)
    }
  }
}
