devsearch-learning
=======================================

The FeatureMining class extracts features from tarballs using a Spark job.

# Extract features from tarballs

* Generate and upload JAR file
	* `sbt assembly`
	* Rename the generated file to `learning.jar` 

* Run Spark feature extractor
    * `spark-submit --num-executors 25 --class devsearch.spark.FeatureMining --master yarn-client learning.jar "hdfs:///projects/devsearch/pwalch/tarballs" "hdfs:///projects/devsearch/pwalch/features/tarballs" "Devsearch_tarballs" > spark_all_log.txt 2>&1`

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
