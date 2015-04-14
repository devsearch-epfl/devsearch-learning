package devsearch

import org.apache.spark.{SparkConf, SparkContext}




/**
 * Created by hubi on 4/12/15.
 */
object FeatureMining {

  //TODO: SET PATH HERE!
  val inputDir = "src/test/resources/blobs"
  val outputDir = "index"


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FeatureMining").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val codeFiles = AstExtractor extract(inputDir)
    val features = CodeEater eat codeFiles


    features map(_.toString) saveAsTextFile(outputDir)
  }
}
