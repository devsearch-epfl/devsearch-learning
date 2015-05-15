devsearch-learning
=======================================

The FeatureMining class extracts features in row format from tarballs using a Spark job.

# Extract features from tarballs

* Generate and upload JAR file
    * `sbt assembly`
    * Rename the generated file to `learning.jar` 

* Run Spark feature extractor
    * Make sure you have tarballs whose size is close to an HDFS block.
    * Run this command (adapt arguments as you like) in a screen session:
        * `spark-submit --num-executors 25 --class devsearch.spark.FeatureMining --master yarn-client learning.jar "hdfs:///projects/devsearch/pwalch/tarballs" "hdfs:///projects/devsearch/pwalch/features" "Devsearch_tarballs" > spark_tarballs.txt 2>&1`

# Split input

The input we received from DevMine contained mostly big files that don't fit in an HDFS block. Since our BlobInputFormat was developed as non-splittable, we needed to adapt the input by splitting it into smaller chunks.

## Script to split big tarballs

The `scripts/split-hdfs.sh` script splits the big tarballs (up to 5GB) we received from DevMine into smaller tarballs that fit in an HDFS block.

This script downloads a big tarball from HDFS, splits it into smaller tarballs using a utility script, and uploads these smaller tarballs back to HDFS.

## Script to split big text files of old format

The `scripts/splitter.pl` script split one megablob (650MB) from `devsearch-concat` into many miniblobs (120MB) such that no file is shared by two parts.

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
