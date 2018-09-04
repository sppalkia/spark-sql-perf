import com.databricks.spark.sql.perf.tpch._

import com.databricks.spark.sql.perf.Query
import com.databricks.spark.sql.perf.ExecutionMode._
import org.apache.commons.io.IOUtils

import org.apache.spark.sql.functions._

val resultLocation = "/Users/alex.behm/spark-sql-perf/TPCH_results"
val scaleFactor = "1"
val format = "parquet"
val databaseName = s"tpch_sf${scaleFactor}_$format"
val tpch = new TPCH (spark.sqlContext)
val iterations = 1
val timeout = 24*60*60 // timeout, in seconds.

// Enable ARC
// spark.conf.set("com.databricks.arc.enabled", true)
// spark.conf.set("spark.sql.columnVector.offheap.enabled", true)

sql(s"use $databaseName")

val experiment = tpch.runExperiment(
  tpch.queries,
  iterations = iterations,
  resultLocation = resultLocation,
  forkThread = true)

experiment.waitForFinish(timeout)

experiment.getCurrentResults.withColumn("Name", substring(col("name"), 2, 100)).withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0).select('Name, 'Runtime).show(false)
