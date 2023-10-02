#!/bin/bash
spark-submit --master local[4] --class cs523.SparkStreaming.Kafka082stream2HBase /home/cloudera/workspace/cs-523-final-project-gp/target/final-project-1.0.0-jar-with-dependencies.jar test
