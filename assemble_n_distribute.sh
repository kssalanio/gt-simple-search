#!/bin/bash
#NOTE: spark jar directory is at [/home/ubuntu/spark/jars/]
# Assembly and copy jar to all nodes
git pull
rm -rf /home/ubuntu/git/gt-simple-search/target/ && ./sbt reload
./sbt update
./sbt assembly

for i in $(cat nodes.lst)
do
    echo "copying jar"
    scp -pr /home/ubuntu/git/gt-simple-search/target/scala-2.11/simplesearch-assembly-0.2.0.jar  $i:~/spark/jars/
#    echo "copying conf folder"
#    scp -pr /home/ubuntu/spark/conf/  $i:~/spark/
#    ssh -tt $i ls -lah /home/ubuntu/spark/jars/simplesearch-assembly-0.2.0.jar
done

