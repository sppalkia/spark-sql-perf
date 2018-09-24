#!/bin/bash

set -x

if [[ -z "${SPARK_HOME}" ]]; then
  echo "SPARK_HOME is not set! Exiting."
  exit 1
fi

${SPARK_HOME}/spark-shell\
  --conf spark.executor.cores=1\
  --conf spark.driver.memory=6g\
  --conf spark.memory.offHeap.enabled=true\
  --conf spark.memory.offHeap.size=6G\
  --conf spark.sql.columnVector.offheap.enabled=true\
  --conf spark.sql.inMemoryColumnarStorage.batchSize=50000\
  --conf spark.sql.shuffle.partitions=16\
  --conf com.databricks.cache.columnarBatch.enabled=true\
  -i run_tpch.scala
