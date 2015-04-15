package devsearch

import devsearch.features.{CodeFileData, Feature, Features}
import org.apache.spark.rdd.RDD

/**
 * Entry point for Spark feature mining script.
 */
object CodeEater {

  /**
   * extracts CodeFileData from CodeFiles
   */
  def toCodeFileData(cf: CodeFile): CodeFileData = {
    cf.data
  }

  /**
   * Eats code and returns distinct features (no duplicates)
   */
  def eat(inputData: RDD[CodeFile]): RDD[Feature] = {
    inputData map toCodeFileData flatMap Features
  }
}
