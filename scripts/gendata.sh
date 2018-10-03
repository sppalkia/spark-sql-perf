#!/bin/bash

$SPARK_HOME/bin/spark-shell\
  --driver-memory 16g\
  --jars $HOME/spark-sql-perf/target/scala-2.11/spark-sql-perf_2.11-0.5.0-SNAPSHOT.jar\
  -i $HOME/spark-sql-perf/src/main/notebooks/TPC-datagen-local.scala
