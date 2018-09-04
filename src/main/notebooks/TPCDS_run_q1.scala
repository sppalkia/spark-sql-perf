import com.databricks.spark.sql.perf.tpcds._

import com.databricks.spark.sql.perf.Query
import com.databricks.spark.sql.perf.ExecutionMode._
import org.apache.commons.io.IOUtils

import org.apache.spark.sql.functions._

val resultLocation = "/home/npoggi/scratch/spark-sql-perf/TPCDS_results"
val scaleFactor = "1" //in GB
val format = "parquet"
val databaseName = s"tpcds_sf${scaleFactor}_$format"

val tpcds = new TPCDS (spark.sqlContext)

val iterations = 1
val randomizeQueries = false
val queryFilter = Seq("q1-v2.4")

def queries = {
  val tpcds2_4Queries = tpcds.queryNames.map { queryName =>
      val queryContent: String = IOUtils.toString(
        getClass().getClassLoader().getResourceAsStream(s"tpcds_2_4/$queryName.sql"))
      new Query(queryName + "-v2.4", spark.sqlContext.sql(queryContent), description = "TPCDS 2.4 Query",
        executionMode = HashResults)
  }
  val filtered_queries = if(queryFilter.nonEmpty) {
    tpcds2_4Queries.filter(q => queryFilter.contains(q.name))
  } else {
    tpcds2_4Queries // tpcds.tpcds2_4Queries
  }
  if (randomizeQueries) scala.util.Random.shuffle(filtered_queries) else filtered_queries
}

val timeout = 24*60*60 // timeout, in seconds.

sql(s"use $databaseName")

val experiment = tpcds.runExperiment(
  queries,
  iterations = iterations,
  resultLocation = resultLocation,
  forkThread = true)

experiment.waitForFinish(timeout)

experiment.getCurrentResults.withColumn("Name", substring(col("name"), 2, 100)).withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0).select('Name, 'Runtime).show(false)
