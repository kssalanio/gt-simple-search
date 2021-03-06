#!/bin/bash
#NOTE: spark jar directory is at [/home/ubuntu/spark/jars/]
# Assembly and copy jar to all nodes
git pull
rm -rf /home/ubuntu/git/gt-simple-search/target/ && ./sbt reload
./sbt update
./sbt assembly

for i in spark00 spark01 spark02
do 
    scp -pr /home/ubuntu/git/gt-simple-search/target/scala-2.11/simplesearch-assembly-0.2.0.jar  $i:~/spark/jars/
    ssh -tt $i ls -lah /home/ubuntu/spark/jars/simplesearch-assembly-0.2.0.jar
done

