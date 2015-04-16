# devsearch-learning

##Offline Spark Job (Feature Mining).

The FeatureMining class transforms BLOB files into features. It processes the BLOBs (each of them around 650MB) individually, extracts "BLOBsnippets" (CodeFiles) and parses them. From the received ASTs it extracts features and saves them as a text file. FeatureMining basically calls all the methods provided in devsearch-ast.
