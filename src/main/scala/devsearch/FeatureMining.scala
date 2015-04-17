package devsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.{Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat


/**
 * Created by hubi on 4/12/15.
 */
object FeatureMining {

  //TODO: SET PATH TO CORRECT REPOSITORY DIRECTORY HERE!
  //val inputDir = "/projects/devsearch/repositories"
  //val inputDir  = "/projects/devsearch/testrepos"
  //val outputDir = "/projects/devsearch/features"
  val inputDir =  "/home/hubi/Documents/BigData/DevSearch/testrepos"
  val outputDir = "/home/hubi/Documents/BigData/DevSearch/features"

  /**
   * We need to process the BLOBs file by file because the header line of each BLOBsnippet gets collected in AstExtractor.
   * Since the BLOBs are so huge this would cause problems if we extracted all BLOBs in parallel.
   * @param args
   */
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FeatureMining")
    implicit val sc = new SparkContext(conf)


    //Go through each language directory and list all the contained BLOBs
    val fs = FileSystem.get(new java.net.URI(inputDir + "/*"), new Configuration())
    val fileList = fs.listStatus(new Path(inputDir))
                     .map(_.getPath)                    //these are all the language directories...
                     .flatMap(p => fs.listStatus(p))    //these are all the files in the language directories
                     .map(_.getPath.toString)


    //process each BLOB
    for(inputFile <- fileList){
      println("\n\n\n\n\n\n\n\nProcessing "+ inputFile +"\n\n\n\n\n\n\n\n")

      //TODO create newAPIHadoopRDD here!
      //When I run the commented code below, I get this strange error: "[Fatal Error] :1:1: Content is not allowed in prolog."
      //Usually this kind of error occurs if an xml file does not begin correctly (<?xml...)
      // I guess, this is because the Configuration object is not initialized correctly...

      
      /*val conf = sc.hadoopConfiguration
      //conf.addResource(new Path(inputFile))
      //FileInputFormat.addInputPath(job, new Path(args[0]));

      val test = sc.newAPIHadoopRDD(conf, classOf[BlobInputFormat], classOf[Text], classOf[Text])

      println("\n\n\n\n\n\n\n\nGenerated "+test.count()+ " snippets.\n\n\n\n\n\n\n\n")
      */


      val codeFiles = AstExtractor extract inputFile

      val features = CodeEater eat codeFiles

      //println("\n\n\n\n\n\n\n\nGenerated "+features.count()+ " features from " + codeFiles.count + " files.\n\n\n\n\n\n\n\n")

      features map(_.toString) saveAsTextFile(outputDir)


    }
  }
}
