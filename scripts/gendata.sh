#!/bin/bash

set -x

# Cleanup - Remove *.class files
# TODO Can we use `sbt clean` for this?
rm -rf project/project/target/
rm -rf project/target/
rm -rf target
rm -rf metastore_db/
rm -rf tpc/

sbt package
../spark/bin/spark-shell\
  --jars ~/work/spark-sql-perf/target/scala-2.11/spark-sql-perf_2.11-0.5.0-SNAPSHOT.jar\
  -i ~/work/spark-sql-perf/src/main/notebooks/TPC-datagen-local.scala
