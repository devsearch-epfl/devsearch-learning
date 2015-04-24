devsearch-learning
=======================================

The `splitter.pl` script splits the megablobs (650MB) from `devsearch-concat` into miniblobs (120MB) such that no file is shared by two parts.

The FeatureMining class extracts features from miniblobs. It processes them individually, extracts CodeFiles and parses them. From the received ASTs it extracts features and saves them as a text file. FeatureMining basically calls all the methods provided in devsearch-ast.


# Script usage

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
    * `hadoop fs -put miniblobs/part-00* /projects/devsearch/pwalch/usable_repos/java`

## Extract features from miniblobs

* Generate and upload JAR file
	* `sbt assembly`
	* Rename the generated file to `learning.jar` 

* Run Spark feature extractor
    * `spark-submit --num-executors 25 --class devsearch.FeatureMining --master yarn-client learning.jar "hdfs:///projects/devsearch/pwalch/usable_repos/java" "hdfs:///projects/devsearch/pwalch/features/java" > spark_all_log.txt 2>&1`

