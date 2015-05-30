# Downloads the files locally and compresses them

filePathList=$(hadoop fs -ls /projects/devsearch/pwalch/datasets/dataset02 | grep -E "^-" | grep -E -o "/projects.*")

for filePath in $filePathList; do
    echo "Downloading: $filePath"
    hadoop fs -get "$filePath"
    fileName=$(basename "$filePath")
    echo "Filename: $fileName"
    gzip "$fileName"
done
