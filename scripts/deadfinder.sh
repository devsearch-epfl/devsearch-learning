# Finds part that could not be extracted in the data set

fileList=`hadoop fs -ls /projects/devsearch/pwalch/datasets/dataset02 | grep -E "^-" | grep -E -o "/projects.*" | grep -o "part-.*-000.tar"`
for id in $(seq 1 738); do
    fileId=`printf %05d $id`
    fileName="part-$fileId-000.tar"
    #echo "$fileName"
    if [[ $fileList =~ $fileName ]]; then
        titi="1"
    else
        echo "Missing: $fileName"
    fi
done