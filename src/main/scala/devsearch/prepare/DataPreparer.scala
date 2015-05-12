package devsearch.prepare

import org.apache.spark.{rdd, SparkConf, SparkContext}
import devsearch.features.Feature
import spray.json._
import spray.json.DefaultJsonProtocol

/**
 * These three classes are needed for transforming RepoRanks and Features into JSON format. (THX Pwalch!)
 */
case class JsonLine(line: String)
object LineJsonProtocol extends DefaultJsonProtocol {
  implicit val jsonNumberFormat = jsonFormat(JsonLine, "$numberInt")
}
case class JsonFeature(feature: String, file: String, line: JsObject)
object FeatureJsonProtocol extends DefaultJsonProtocol {
  implicit val jsonFeatureFormat = jsonFormat3(JsonFeature)
}
case class JsonRepoRank(ownerRepo: String, score: Double)
object RepoRankJsonProtocol extends DefaultJsonProtocol {
  implicit val jsonRepoRankFormat = jsonFormat2(JsonRepoRank)
}

//spark-submit --num-executors 35 --class "devsearch.prepare.DataPreparer" --master yarn-client "devsearch-learning-assembly-0.1.jar" "/projects/devsearch/pwalch/features/*/*" "/projects/devsearch/ranking/*" "/projects/devsearch/JsonBuckets" 5


/**
 * Created by hubi on 5/1/15.
 *
 * The dataPreparer is needed for converting to JSON and distributing our data (features, RepoRank) to several buckets.
 * It basically transforms the python scripts features2json.py and distributeFiles.py into a spark job.
 *
 * The buckets are chosen by hashing "owner/repo" of the features and ranks. Each of the Akka PartitionLookup nodes will
 * be responsible for one of those buckets.
 *
 * This spark job applies 'consistent hashing' to our features. More about consistent hashing on
 * http://www.tom-e-white.com//2007/11/consistent-hashing.html.
 * The algorithm used in our case is the one of JosephMoniz: http://blog.plasmaconduit.com/consistent-hashing/ and
 * https://github.com/JosephMoniz/scala-hash-ring and
 *
 *
 * Usage:
 * - 1st argument is the input directory containing the feature files
 * - 2nd argument is the input directory containing the repoRank files
 * - 3rd argument is the output directory of the buckets
 * - 4th argument is the number of buckets
 */
object DataPreparer {


  def main(args: Array[String]) {

    //some argument checking...
    if(args.length != 4)
      throw new ArgumentException("You need to enter 4 arguemnts, not " + args.length + ". ")
    if(!args(3).matches("""\d+"""))
      throw new ArgumentException("4th argument must be an integer.")

    val featureInput  = args(0)
    val repoRankInput = args(1)
    val outputPath    = args(2)
    val nbBuckets     = args(3).toInt



    val sparkConf = new SparkConf().setAppName("Data Splitter").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)


    //prepare consistent hashing
    //bucket0 is always empty!
    val buckets = (0 to nbBuckets).tail.map(nb => HashRingNode("bucket"+nb, 100))
    val ring = new SerializableHashRing(buckets)


    //read files
    val features = sc.textFile(featureInput)
    val ranks    = sc.textFile(repoRankInput)


    //transform into JSON and assign each line to a bucket.
    //The bucket is chosen according to ownerRepo.
    val featuresJSON = features.map(Feature.parse(_)).collect{ case f if f.key.length < 512 => {
      import FeatureJsonProtocol._
      import LineJsonProtocol._
      val feature = f.key
      val owner   = f.pos.location.user
      val repo    = f.pos.location.repoName
      val file    = f.pos.location.fileName
      val jsonFeature = JsonFeature(
        feature,
        owner + "/" + repo + "/" + file,
        JsonLine(f.pos.line.toString).toJson.asJsObject
      ).toJson.asJsObject.toString

      (ring.get(owner + "/" + repo).get, jsonFeature)
    }}

    val ranksJSON = ranks.collect{
      case r if r.takeWhile(_ != ',').length < 512 => {
        val splitted = r.split(",")
        import RepoRankJsonProtocol._
        val jsonRepoRank = JsonRepoRank(splitted(0), splitted(1).toDouble).toJson.asJsObject.toString

        (ring.get(splitted(0)).get, jsonRepoRank)
      }
    }


    //Since bucket 0 is always empty, only go from 1 to nbBuckets
    for (i <- 1 to nbBuckets) {
      featuresJSON.filter(_._1 == "bucket" + i).map(_._2).saveAsTextFile(outputPath + "/features/bucket" + i)
      ranksJSON.filter(_._1 == "bucket" + i).map(_._2).saveAsTextFile(outputPath + "/repoRank/bucket" + i)
    }
  }
}


//Thrown when arguments are somehow incorrect...
case class ArgumentException(cause:String)  extends Exception("ERROR: " + cause + """
    |        Correct usage:
    |         - arg1 = path/to/features
    |         - arg2 = path/to/repoRank
    |         - arg4 = path/to/bucket/output
    |         - arg3 = nbBuckets (Integer)
  """.stripMargin)
