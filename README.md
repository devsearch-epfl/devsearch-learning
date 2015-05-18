devsearch-learning
=======================================

The FeatureMining class extracts features from tarballs using a Spark job.

# Extract features from tarballs

* Generate and upload JAR file
	* `sbt assembly`
	* Rename the generated file to `learning.jar` 

* Run Spark feature extractor
    * `spark-submit --num-executors 25 --class devsearch.spark.FeatureMining --master yarn-client learning.jar "hdfs:///projects/devsearch/pwalch/tarballs" "hdfs:///projects/devsearch/pwalch/features/tarballs" "Devsearch_tarballs" > spark_tarballs.txt 2>&1`

# Script usage to generate miniblobs (old format)

The `splitter.pl` script splits the megablobs (650MB) from `devsearch-concat` into miniblobs (120MB) such that no file is shared by two parts.

## Split DevMine data (megablobs) into smaller blobs (miniblobs)

* Create a small folder architecture in your HOME:
    * `mkdir languages && cd languages`
    * `mkdir java && cd java`

* Download the mega-blobs from HDFS to a new `megablobs` folder
    `hadoop fs -get /projects/devsearch/repositories/java megablobs`

* Create an output folder for the script
    * `mkdir miniblobs`

* Run the script on each megablob:
    * `find megablobs -type f -exec perl splitter.pl {} miniblobs 120M \;`

* Remove miniblobs that are too big for a split
    * `find miniblobs -size +121M -type f -exec rm {} \;`

* Put files back on HDFS
    * `hadoop fs -mkdir /projects/devsearch/pwalch/usable_repos/java`
    * `hadoop fs -put miniblobs/part-* /projects/devsearch/pwalch/usable_repos/java`

===================================================================================

# DataPreparer
The DataPreparer is used for joining the features with the repoRank, counting the features, convert everything into JSON and split it into different buckets. Further it generates some stats if wished. 

## Usage

* Use `learning.jar` (generated as discribed above)
* The DataPreparer is executed with the following arguments:
    * 1st argument is the input directory containing the feature files
    * 2nd argument is the input directory containing the repoRank files
    * 3rd argument is the output directory of the buckets
    * 4th argument is the minimum number of counts. Features with a lower count are not being saved in the featureCount.
    * 5th argument is the number of buckets
    * 6th argument tells if stats shall be created or not. ('Y' = define stats)
* Run Spark DataPreparer `spark-submit --num-executors 340 --driver-memory 4g --executor-memory 4g --class "devsearch.prepare.DataPreparer" --master yarn-client "devsearch-learning-assembly-0.1.jar" "/projects/devsearch/pwalch/features/dataset01/*/*,/projects/devsearch/pwalch/features/dataset02/*" "/projects/devsearch/ranking/*" "/projects/devsearch/testJsonBuckets" 100 15 y` 
