#!/bin/sh

spark-submit --master yarn --deploy-mode client --conf spark.driver.memory=27g --class it.unisa.sci.App --num-executors 8 --executor-memory 27g --executor-cores 7 target/scispark-1.0-SNAPSHOT.jar
