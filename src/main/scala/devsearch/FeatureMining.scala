package devsearch

import devsearch.features.CodeFileData
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
    val files = blobPathList.foldLeft(keyValueAccumulator)((acc, path) =>
      sc.union(acc, sc.newAPIHadoopFile(path, classOf[BlobInputFormat], classOf[Text], classOf[Text]))
    )

    val codeFiles = AstExtractor.extract(files)

    codeFiles.foreach {
      case codeFile: CodeFileData =>
        val location = codeFile.location
        println(codeFile.language + ":" + location.user + "/" + location.repoName + location.fileName + ":" + codeFile.ast)
    }

//    val features = CodeEater.eat(codeFiles)
//
//    features.flatMap(_.toString).saveAsTextFile(outputDir)

    //process each BLOB
//    for (inputFile <- blobPathList) {
//      println("Processing " + inputFile)
//      println("Generated " + .count() + " snippets.")
//      val codeFiles = AstExtractor extract inputFile
//      val features = CodeEater eat codeFiles
//      //println("\n\n\n\n\n\n\n\nGenerated "+features.count()+ " features from " + codeFiles.count + " files.\n\n\n\n\n\n\n\n")
//      features map(_.toString) saveAsTextFile(outputDir)
//    }
  }
}
