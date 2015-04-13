package devsearch

import devsearch.ast._
import devsearch.features._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Entry point for Spark feature mining script.
 */
object CodeEater {
  /**
   * Eats code and returns features
   * XXX: features aren't explicitly made distinct, but the collection methods on
   *      the AST used during feature extraction create feature sets, so identical
   *      features (ie, same file, same position) will be removed by set semantics.
   */
  def eat(inputData: RDD[CodeFileData]): RDD[Feature] = {
    inputData.flatMap(Features)
  }
}
