#!/bin/sh

# Splits big tarballs from HDFS into smaller ones

echo "Starting"

start="1"
end="742"
for i in `seq $start $end`; do
    fileId=`printf %05d $i`
    #echo "Current id: $fileId"
    fileName="part-$fileId"
    #echo "File name without extension: $fileName"
    extendedFileName="$fileName.tar"
    echo "Downloading new file: $extendedFileName"
    hadoop fs -get /projects/devsearch/finalBlobs/$extendedFileName
    ./split-tar.sh -s 100M $extendedFileName
    find . -name "*.tar" -size +125M -delete

    outputWildcard="$fileName-*.tar"
    #echo "Wildcard for splitted files: $outputWildcard"
    if [ `find . -name "$outputWildcard" | wc -l` -ge 1 ]; then
        echo "Splitted successfully for upload: $extendedFileName"
        hadoop fs -put $outputWildcard /projects/devsearch/pwalch/final_tarballs
    else
        echo "Failed to split file: $extendedFileName"
    fi
    find . -name "*.tar" -delete
done

echo "Done"