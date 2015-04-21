package devsearch

import devsearch.ast.Empty.NoDef
import devsearch.features.{FeatureRecognizer, CodeFileData}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text

object FeatureMining {

  def mine(inputDir: String, outputDir: String) {
    val conf = new SparkConf().setAppName("FeatureMining").setMaster("local[2]")
    implicit val sc = new SparkContext(conf)

    // Go through each language directory and list all the contained blobs
    val fs = FileSystem.get(new java.net.URI(inputDir + "/*"), new Configuration())
    val blobPathList = fs.listStatus(new Path(inputDir))
        // Language directories
        .map(_.getPath)
        // Files in the language directories
        .flatMap(p => fs.listStatus(p))
        .map(_.getPath.toString)

    // Use custom input format to get header/snippet pairs from part files
    val headerSnippetAcc: RDD[(Text, Text)] = sc.emptyRDD[(Text, Text)]
    val headerSnippetPairs = blobPathList.foldLeft(headerSnippetAcc)((acc, path) =>
      sc.union(acc, sc.newAPIHadoopFile(path, classOf[BlobInputFormat], classOf[Text], classOf[Text]))
    )

    // Generate code files and remove those that don't have an AST
    val codeFiles = AstExtractor.extract(headerSnippetPairs).filter(c => c.ast != NoDef)

    // Extract and store features
    val features = codeFiles.flatMap(FeatureRecognizer)
    features.map(_.encode).saveAsTextFile(outputDir)
  }

  /**
   * We need to process the BLOBs file by file because the header line of each BLOBsnippet gets collected in AstExtractor.
   * Since the BLOBs are so huge this would cause problems if we extracted all BLOBs in parallel.
   */
  def main(args: Array[String]) {
    val inputDir = "/Users/pierre/Documents/myfiles/EPFL/ProjetBigData/spark_input"
    val outputDir = "/Users/pierre/Documents/myfiles/EPFL/ProjetBigData/spark_output/my_output"

    println("Starting to mine")
    mine(inputDir, outputDir)
    println("Mining done")
  }
}
