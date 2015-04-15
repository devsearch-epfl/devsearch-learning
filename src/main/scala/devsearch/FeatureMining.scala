package devsearch

import org.apache.spark.{SparkConf, SparkContext}




/**
 * Created by hubi on 4/12/15.
 */
object FeatureMining {

  //TODO: SET PATH HERE!
  //val inputDir = "/projects/devsearch/repositories/*"
  val inputDir  = "/projects/devsearch/testrepos/java"
  val outputDir = "/projects/devsearch/features"


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FeatureMining")
    implicit val sc = new SparkContext(conf)
    val codeFiles = AstExtractor extract inputDir

    println("\n\n\n\n\n\n\n\nnbCodeFiles: "+codeFiles.count()+"\n\n\n\n\n\n\n")

    val features = CodeEater eat codeFiles



    println("\n\n\n\n\n\n\n\nnbFeatures: "+features.count()+"\n\n\n\n\n\n\n")


    features map(_.toString) saveAsTextFile(outputDir)
  }
}
