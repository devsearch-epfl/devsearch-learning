package devsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text

object FeatureMining {

  //TODO: SET PATH TO CORRECT REPOSITORY DIRECTORY HERE!
  val inputDir = "/Users/pierre/Documents/myfiles/EPFL/ProjetBigData/spark_input"
  val outputDir = "/Users/pierre/Documents/myfiles/EPFL/ProjetBigData/spark_output"

  /**
   * We need to process the BLOBs file by file because the header line of each BLOBsnippet gets collected in AstExtractor.
   * Since the BLOBs are so huge this would cause problems if we extracted all BLOBs in parallel.
   */
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FeatureMining").setMaster("local[2]")
    implicit val sc = new SparkContext(conf)

    // Go through each language directory and list all the contained BLOBs
    val fs = FileSystem.get(new java.net.URI(inputDir + "/*"), new Configuration())
    val fileList = fs.listStatus(new Path(inputDir))
        // Language directories
        .map(_.getPath)
        // Files in the language directories
        .flatMap(p => fs.listStatus(p))
        .map(_.getPath.toString)

    //process each BLOB
    for (inputFile <- fileList) {
      println("Processing " + inputFile)

      val test = sc.newAPIHadoopFile(inputDir, classOf[BlobInputFormat], classOf[Text], classOf[Text])

      println("Generated " + test.count() + " snippets.")

      /*
            val codeFiles = AstExtractor extract inputFile
            val features = CodeEater eat codeFiles
            //println("\n\n\n\n\n\n\n\nGenerated "+features.count()+ " features from " + codeFiles.count + " files.\n\n\n\n\n\n\n\n")
            features map(_.toString) saveAsTextFile(outputDir)
      */

    }
  }
}
