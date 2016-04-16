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
case class JsonFeature(feature: String, file: String, line: JsObject, repoRank: Double)
object FeatureJsonProtocol extends DefaultJsonProtocol {
  implicit val jsonFeatureFormat = jsonFormat4(JsonFeature)
}
case class JsonRepoRank(ownerRepo: String, score: Double)
object RepoRankJsonProtocol extends DefaultJsonProtocol {
  implicit val jsonRepoRankFormat = jsonFormat2(JsonRepoRank)
}
case class JsonCount(feature: String, language: String, count: JsObject)
object CountJsonProtocol extends DefaultJsonProtocol {
  implicit val jsonCountFormate = jsonFormat3(JsonCount)
}

case class FileIndex(partitionIndex: Int, innerIndex: Int)



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
 *
 * MAKE SURE to give the job enough resources!
 */
// Example:
// spark-submit --num-executors 340 --driver-memory 4g --executor-memory 4g --class "devsearch.prepare.DataPreparer" --master yarn-client "devsearch-learning-assembly-0.1.jar" "/projects/devsearch/pwalch/features/dataset01/*/*,/projects/devsearch/pwalch/features/dataset02/*" "/projects/devsearch/ranking/*" "/projects/devsearch/testJsonBuckets" 100 15 y

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
    val features = sc.textFile(featureInput).distinct
    val ranks    = sc.textFile(repoRankInput)


    val ranksKVPair = ranks.map {r =>
      val splitted = r.split(",")
      (splitted(0), splitted(1).toDouble)
    }.cache

    //transform features into a key-value pair (filter out features whose key is too long. This makes Mongo crash.)
    val featuresBucket = features.map(Feature.parse(_)).collect{case f if f.key.length < 512 => {
      val feature   = f.key
      val ownerRepo = f.pos.location.user +"/"+ f.pos.location.repoName
      val file      = f.pos.location.fileName
      val line      = f.pos.line.toString

      (ownerRepo, (ring.get(ownerRepo + "/" + file).get, file, line, feature))
    }}.cache


    // yields a (fileIndex, featureContent) RDD
    // where fileIndex is composed of two integers and is unique per repository
    // some repositories might have duplicated index if features from the same file are split on several spark partitions
    val fileIndexed = featuresBucket

      // transform our features in a more usable way
      .map{case (repo, (bucket, file, line, feature)) => (repo + "/" + file, bucket, line, feature, repo, file)}

      // we sort all features by full github path file
      .sortBy((featureEntity) => featureEntity._1)

      // we map each spark partition to a unique index
      .mapPartitionsWithIndex((partitionIndex, featureEntityIterator) => {

        var idx = 0
        val lastElement = ""

        // each
        featureEntityIterator.map((featureEntity) => {
          if (lastElement != featureEntity._1)
            idx = 1 + idx

          (FileIndex(partitionIndex, idx), featureEntity)
        })
      })

    print(fileIndexed.toDebugString)


    // we extract the fileIndex from what we just built
    val fileIndex = fileIndexed
      .map{case (index: FileIndex, featureEntity) => (index, featureEntity._1)}
      .distinct()

    fileIndexed.saveAsTextFile(outputPath + "/normalized/features")
    fileIndex.saveAsTextFile(outputPath + "/normalized/index")




    //Join features with repoRank and transform them into JSON
    //Since bucket 0 is always empty, only go from 1 to nbBuckets
//    for (i <- 1 to nbBuckets) {
//
//      //Set a high level of parallelism! otherwise the join will fail due to a OutOfMemoryError.
//      featuresBucket.filter(_._2._1 == "bucket" + i).leftOuterJoin(ranksKVPair, 100).map{case (ownerRepo, ((bucket, file, line, feature), score)) =>
//        import NumberIntJsonProtocol._
//        import FeatureJsonProtocol._
//        val json = JsonFeature(feature,
//          ownerRepo +"/"+ file,
//          JsonNumberInt(line).toJson.asJsObject,
//          score.getOrElse(0.0)).toJson.asJsObject.toString
//        (bucket, json)
//      }.saveAsTextFile(outputPath + "/features/bucket" + i)
//    }







    //count the nb occurrences of each feature per language and bucket and create a JSON string:
    //first transform it into key-value pair, then sum up.
//    val partitionCount = featuresBucket.map{
//      case (ownerRepo, (bucket, file, line, feature)) => {
//        val language = file.substring(file.lastIndexOf('.') + 1)
//        ((bucket, feature, language), 1)
//      }
//    }.reduceByKey(_+_)
//
//    //sum up partitionCounts for getting the global count:
//    val globalCount = partitionCount.map{case ((bucket, feature, language), count) => ((feature, language), count)}
//      .reduceByKey(_+_)
//
//    val globalCountJsonString = globalCount.filter(_._2 >= minCount)
//      .map{case ((feature, language), count) => {
//      import CountJsonProtocol._
//      import NumberIntJsonProtocol._
//      JsonCount(feature,
//        language,
//        JsonNumberInt(count.toString).toJson.asJsObject)
//        .toJson.asJsObject.toString
//    }}
//
//
//    //transform partitionCount into JSON
//    val partitionCountJsonString = partitionCount.filter(_._2 >= minCount).map{
//      case ((bucket, feature, language), count) =>
//        import NumberIntJsonProtocol._
//        import CountJsonProtocol._
//        (bucket, JsonCount(feature, language, JsonNumberInt(count.toString).toJson.asJsObject).toJson.asJsObject.toString)
//    }
//
//    globalCountJsonString.saveAsTextFile(outputPath + "/globalCount")
//
//    for (i <- 1 to nbBuckets) {
//      partitionCountJsonString.filter(_._1 == "bucket" + i).map(_._2).saveAsTextFile(outputPath + "/partitionCounts/bucket" + i)
//    }
//
//
//    //create some statistics if wanted...
//    if(createStats){
//
//      //merge languages. we don't need to separate between languages for the stats.
//      val globCountLangMerged = globalCount.map{case ((feature, language), count) => (feature, count)}
//        .reduceByKey(_+_)
//
//      //count how many different features there are per count. (e.g. 500 different features have been counted 75 times)
//      val nbFeaturesPerCount = globCountLangMerged.map{case (feature, count) => (count, 1)}
//        .reduceByKey(_+_)
//        .sortByKey(true)
//        .map{case (count, nbFeatures) => count +","+ nbFeatures}
//
//      nbFeaturesPerCount.saveAsTextFile(outputPath + "/stats/nbFeaturesPerCount")
//
//      //first give all elems the same key, then sum them all up
//      globCountLangMerged.map(c => ("all", c._2))
//        .reduceByKey(_+_)
//        .map(c => "total feature count: " + c._2)
//        .saveAsTextFile(outputPath + "/stats/totalCount")
//
//      //get the most frequent features (dirty hack for saving as file in HDFS)
//      sc.parallelize(globCountLangMerged.map{case (feature, count) => (count, feature)}
//        .takeOrdered(100)(Ordering[Int].reverse.on(_._1))
//        .map{case (count, feature) => feature +","+ count}
//      ).saveAsTextFile(outputPath + "/stats/top100")
//
//
//
//    }
  }
}


/**
 * Thrown when arguments are somehow incorrect...
 */
case class ArgumentException(cause:String)  extends Exception("ERROR: " + cause + """        Correct usage:
                                                                                    |         - arg1 = path/to/features
                                                                                    |         - arg2 = path/to/repoRank
                                                                                    |         - arg3 = path/to/bucket/output
                                                                                    |         - arg4 = min count (Integer)
                                                                                    |         - arg5 = nbBuckets (Integer)
                                                                                    |         - arg6 = 'y' if stats shall be created.
                                                                                  """.stripMargin)