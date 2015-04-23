package devsearch

import devsearch.features.FeatureRecognizer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text

object FeatureMining {

  def mine(inputDir: String, outputDir: String) {
    val conf = new SparkConf().setAppName("FeatureMining")
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
    val headerSnippetPairs = sc.union(
      blobPathList.map(path =>
      sc.newAPIHadoopFile(path, classOf[BlobInputFormat], classOf[Text], classOf[Text])
      )
    )

    // Generate code files and remove those that don't have an AST
    val codeFiles = AstExtractor.extract(headerSnippetPairs)

    // Extract and store features
    val features = codeFiles.flatMap(FeatureRecognizer)
    features.map(_.encode).saveAsTextFile(outputDir)
  }

  /**
   * We need to process the BLOBs file by file because the header line of each BLOBsnippet gets collected in AstExtractor.
   * Since the BLOBs are so huge this would cause problems if we extracted all BLOBs in parallel.
   */
  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Invalid arguments. Args are: input_dir output_dir")
      return
    }
    // val javaInputDir = "hdfs:///projects/devsearch/pwalch/usable_repos/java"
    // val javaOutputDir = "hdfs:///projects/devsearch/pwalch/features/java"

    val inputDir = args(0)
    val outputDir = args(1)
    mine(inputDir, outputDir);

    println("Mining done")
  }
}
