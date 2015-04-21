package devsearch.utils

import org.scalatest._
import devsearch.CodeEater
import devsearch.JavaFile
import devsearch.GoFile
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by hubi on 4/15/15.
 */
class CodeEaterTest extends FlatSpec {

  "CodeEater" should "not throw exception" in {
    val conf = new SparkConf().setAppName("FeatureMining").setMaster("local[2]")
    implicit val sc = new SparkContext(conf)

    val codeFiles = List(new JavaFile(19, "repoOwner", "repoName", "filePath", "public class Test{}"), new GoFile(74, "repoOwner", "repoName", "otherFilePath", """package main|   fmt.Printf("hello world!\n")|}""".stripMargin))
    val rdd = sc.parallelize(codeFiles)
    CodeEater.eat(rdd);
  }
}
