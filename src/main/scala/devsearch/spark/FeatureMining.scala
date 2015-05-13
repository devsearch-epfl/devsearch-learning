package devsearch.spark

import devsearch.features.{Feature, FeatureRecognizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text

object FeatureMining {
  def mine(inputDir: String, outputDir: String, jobName: String) {
    val conf = new SparkConf().setAppName(jobName).set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)

    // Recursively list files
    val fs = FileSystem.get(new java.net.URI(inputDir + "/*"), new Configuration())
    val blobPathList = fs.listStatus(new Path(inputDir))
        // Language directories
        .map(_.getPath)
        // Files in the language directories
        .flatMap(p => fs.listStatus(p))
        .map(_.getPath.toString)
        .toList

    val headerSnippetPairs = readInput(sc, blobPathList)
    val features = extractFeatures(headerSnippetPairs)
    features.map(_.encode).saveAsTextFile(outputDir)
  }

  def readInput(sc: SparkContext, files: List[String]): RDD[(Text, Text)] = {
    sc.union(
      files.map(path =>
        sc.newAPIHadoopFile(path, classOf[BlobInputFormat], classOf[Text], classOf[Text])
      )
    )
  }

  def extractFeatures(headerSnippetPairs: RDD[(Text, Text)]): RDD[Feature] = {
    // Generate code files and remove those that don't have an AST
    val codeFiles = AstExtractor.extract(headerSnippetPairs)

    // Extract features from ASTs return them
    codeFiles.flatMap { file =>
      try {
        FeatureRecognizer(file)
      } catch {
        case e: OutOfMemoryError => None
      }
    }
  }

  /**
   * We need to process the BLOBs file by file because the header line of each BLOBsnippet
   * gets collected in AstExtractor.
   * Since the BLOBs are so huge this would cause problems if we extracted all BLOBs in parallel.
   */
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
    mine(inputDirNoSlash, outputDir, jobName)

    println("Mining done")
  }
}
