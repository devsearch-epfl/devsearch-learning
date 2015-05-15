package devsearch.prepare


import devsearch.features.Feature
import spray.json._
import spray.json.DefaultJsonProtocol
import org.apache.spark.{SparkConf, SparkContext}

/**
 * These three classes are needed for transforming RepoRanks and Features into JSON format. (THX Pwalch!)
 */
case class JsonNumberInt(line: String)
object NumberIntJsonProtocol extends DefaultJsonProtocol {
  implicit val jsonNumberFormat = jsonFormat(JsonNumberInt, "$numberInt")
}
case class JsonFeature(feature: String, file: String, line: JsObject)
object FeatureJsonProtocol extends DefaultJsonProtocol {
  implicit val jsonFeatureFormat = jsonFormat3(JsonFeature)
}
case class JsonRepoRank(ownerRepo: String, score: Double)
object RepoRankJsonProtocol extends DefaultJsonProtocol {
  implicit val jsonRepoRankFormat = jsonFormat2(JsonRepoRank)
}
case class JsonCount(feature: String, language: String, count: JsObject)
object CountJsonProtocol extends DefaultJsonProtocol {
  implicit val jsonCountFormate = jsonFormat3(JsonCount)
}



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
 * - 4th argument is the minimum number of counts. Features with a lower count are not being saved in the featureCount.
 * - 5th argument is the number of buckets
 * - 6th argument tells if stats shall be created or not. ('Y' = define stats)
 */
 // Example:
 // spark-submit --num-executors 200 --class "devsearch.prepare.DataPreparer" --master yarn-client "devsearch-learning-assembly-0.1.jar" "/projects/devsearch/pwalch/features/*/*" "/projects/devsearch/ranking/*" "/projects/devsearch/testJsonBuckets" 3 5 y

object DataPreparer {


  def main(args: Array[String]) {

    //some argument checking...
    if(args.length != 6)
      throw new ArgumentException("You need to enter 6 arguemnts, not " + args.length + ". ")
    if(!args(3).matches("""\d+"""))
      throw new ArgumentException("4th argument must be an integer.")
    if(!args(4).matches("""\d+"""))
      throw new ArgumentException("5th argument must be an integer.")

    val featureInput  = args(0)
    val repoRankInput = args(1)
    val outputPath    = args(2)
    val minCount      = args(3).toInt
    val nbBuckets     = args(4).toInt
    val createStats   = (args(5).equals("y"))



    val sparkConf = new SparkConf().setAppName("Data Splitter")
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
    val featuresJSON= features.map(Feature.parse(_)).collect{ case f if f.key.length < 512 => {
      import NumberIntJsonProtocol._
      val feature  = f.key
      val owner    = f.pos.location.user
      val repo     = f.pos.location.repoName
      val file     = f.pos.location.fileName
      val bucket   = ring.get(owner + "/" + repo).get
      val jsonFeature = JsonFeature(
        feature,
        owner + "/" + repo + "/" + file,
        JsonNumberInt(f.pos.line.toString).toJson.asJsObject
      )

      (bucket, jsonFeature)
    }}

    //transform features into JSON
    val featuresJsonString = featuresJSON.map{
      import FeatureJsonProtocol._
      f => (f._1, f._2.toJson.asJsObject.toString)
    }



    //transform repoRank into JSON
    val ranksJsonString = ranks.collect{
      case r if r.takeWhile(_ != ',').length < 512 => {
        val splitted = r.split(",")
        import RepoRankJsonProtocol._
        val jsonRepoRank = JsonRepoRank(splitted(0), splitted(1).toDouble).toJson.asJsObject.toString

        (ring.get(splitted(0)).get, jsonRepoRank)
      }
    }



    //count the nb occurrences of each feature per language and bucket and create a JSON string:
    //first transform it into key-value pair, then sum up.
    val partitionCount = featuresJSON.map{
      case (bucket, JsonFeature(feature, ownerRepoFile, line)) => {
        val language = ownerRepoFile.substring(ownerRepoFile.lastIndexOf('.') + 1)
        ((bucket, feature, language), 1)
      }
    }.reduceByKey(_+_)

    //sum up partitionCounts for getting the global count:
    val globalCount = partitionCount.map{case ((bucket, feature, language), count) => ((feature, language), count)}
                                    .reduceByKey(_+_)

    val globalCountJsonString = globalCount.filter(_._2 >= minCount)
                                           .map{case ((feature, language), count) => {
                                             import CountJsonProtocol._
                                             import NumberIntJsonProtocol._
                                             JsonCount(feature,
                                                       language,
                                                       JsonNumberInt(count.toString).toJson.asJsObject)
                                                         .toJson.asJsObject.toString
                                           }}


    //transform partitionCount into JSON
    val partitionCountJsonString = partitionCount.filter(_._2 >= minCount).map{
      case ((bucket, feature, language), count) =>
        import NumberIntJsonProtocol._
        import CountJsonProtocol._
        (bucket, JsonCount(feature, language, JsonNumberInt(count.toString).toJson.asJsObject).toJson.asJsObject.toString)
    }



    //Since bucket 0 is always empty, only go from 1 to nbBuckets
    for (i <- 1 to nbBuckets) {
      featuresJsonString.filter(_._1 == "bucket" + i).map(_._2).saveAsTextFile(outputPath + "/features/bucket" + i)
    }

    for (i <- 1 to nbBuckets) {
      ranksJsonString.filter(_._1 == "bucket" + i).map(_._2).saveAsTextFile(outputPath + "/repoRank/bucket" + i)
    }

    globalCountJsonString.saveAsTextFile(outputPath + "/globalCount")

    for (i <- 1 to nbBuckets) {
      partitionCountJsonString.filter(_._1 == "bucket" + i).map(_._2).saveAsTextFile(outputPath + "/partitionCounts/bucket" + i)
    }


    //create some stats
    if(createStats){

      //count how many different features there are per count. (e.g. 500 different features have been counted 75 times)
      val nbFeaturesPerCount = globalCount.map{case ((feature, language), count) => (count, 1)}
                                          .reduceByKey(_+_)
                                          .sortByKey(true)
                                          .map{case (count, nbFeatures) => count +","+ nbFeatures}

      nbFeaturesPerCount.saveAsTextFile(outputPath + "/stats/nbFeaturesPerCount")

      //first give all elems the same key, then sum them all up
      globalCount.map(c => ("all", c._2))
                 .reduceByKey(_+_)
                 .map(c => "total feature count: " + c._2)
                 .saveAsTextFile(outputPath + "/stats/totalCount")

      //get the most frequent features (dirty hack for saving as file in HDFS)
      sc.parallelize(globalCount.map{case ((feature, language), count) => (count, (feature, language))}
                                .takeOrdered(100)
                                .map{case (count, (feature, language)) => feature +"("+ language +"),"+ count }
                    ).saveAsTextFile(outputPath + "/stats/mostFrequent")



    }
  }
}


//Thrown when arguments are somehow incorrect...
case class ArgumentException(cause:String)  extends Exception("ERROR: " + cause + """        Correct usage:
                                                                                    |         - arg1 = path/to/features
                                                                                    |         - arg2 = path/to/repoRank
                                                                                    |         - arg3 = path/to/bucket/output
                                                                                    |         - arg4 = min count (Integer)
                                                                                    |         - arg5 = nbBuckets (Integer)
                                                                                    |         - arg6 = 'y' if stats shall be created.
                                                                                  """.stripMargin)