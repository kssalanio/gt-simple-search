#!/bin/bash

# Restart HDFS
stop-dfs.sh
start-dfs.sh

# Restart Spark daemons
/home/ubuntu/spark/sbin/stop-all.sh
/home/ubuntu/spark/sbin/start-all.sh