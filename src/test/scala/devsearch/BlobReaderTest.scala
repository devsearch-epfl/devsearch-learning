package devsearch


import java.nio.file.Files

import org.apache.hadoop.io.Text
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, FlatSpec}

class BlobReaderTest extends FlatSpec with Matchers {
  "The hadoop input format" should "retrieve the correct code fomr the blob" in {
      val rootDir = Files.createTempDirectory("root")
      val langDir = Files.createDirectory(rootDir.resolve("lang"))

      val src = Files.createFile(langDir.resolve("src.scala"))

      val content =
        """
          |object Main {
          |
          |   def main(args : Array[String]) : Unit = {
          |     println("Hello world!")
          |   }
          |}
        """.stripMargin

      Files.write(src, content.getBytes("UTF-8"))

      val outDir = Files.createTempDirectory("out")

      devsearch.concat.Main.main(Array("-j", "1") ++ Array(rootDir, outDir).map(_.toAbsolutePath.toString))


      val resFile = outDir.toFile.listFiles()(0).listFiles()(0)

      val conf = new SparkConf().setMaster("local[1]").setAppName("BlobReaderTest")

      val sc = new SparkContext(conf)


      val rdd = sc.newAPIHadoopFile(resFile.getAbsolutePath, classOf[BlobInputFormat], classOf[Text], classOf[Text])

      val arr = rdd.map{ case (key, value) => (key.toString, value.toString) }collect()


      arr.size should equal(1)
      val (key, value) = arr(0)

      key should equal(rootDir.relativize(src).toString)
      value should equal(content)
  }

}