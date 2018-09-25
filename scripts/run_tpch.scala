
// Configuration.
val queries = Seq(6) //Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22)
val enableArc = false
val runProfile = true
val runs = 3

/////////////////////////////////////////////////////////////////

// Debugging conf
// spark.conf.set("weld.compile.traceExecution", "true")
spark.conf.set("weld.compile.dumpCode", true)
spark.conf.set("weld.log.level", "warn")

// Arc
spark.conf.set("com.databricks.arc.enabled", enableArc)
spark.conf.set("weld.memory.limit", 1024L * 1024L * 1024L * 10)

// Spark
spark.conf.set("spark.memory.offHeap.enabled", true)
spark.conf.set("spark.memory.offHeap.size", "16G")
spark.conf.set("spark.sql.columnVector.offheap.enabled", true)
spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 50000)
spark.conf.set("spark.sql.shuffle.partitions", 16)
spark.conf.set("com.databricks.cache.columnarBatch.enabled", true)
spark.conf.set("spark.databricks.io.parquet.nativeReader.enabled", false)

spark.sql("use tpch_sf10_parquet")

for (queryNum <- queries) {
  val tpchBaseDir = "/home/ubuntu/spark/sql/core/target/scala-2.11/test-classes/tpch/"
  val query = scala.io.Source.fromFile(s"${tpchBaseDir}/q${queryNum}.sql").getLines mkString "\n"

  if (enableArc) {
    println(Console.BLUE + s"Running Query $queryNum with Arc" + Console.RESET)
  } else {
    println(Console.BLUE + s"Running Query $queryNum with Spark WSCG" + Console.RESET)
  }

  // Print top few rows to verify correctness.
  println(Console.GREEN + s"Printing Top 10 Rows for Correctness:" + Console.RESET)
  spark.sql(query).take(10).foreach(println)

  for (q <- 1 to runs) {
    spark.sql(query).time()
  }

  if (runProfile) {
    println(Console.BLUE + "Running profile..." + Console.RESET)
    import com.databricks.spark.profiler.RunParameters
    val profiler = org.apache.spark.SparkEnv.get.profiler
    profiler.warmUp()
    val run = profiler.run(RunParameters(format = "tree=total", location = Some("profiles")))

    // Run the query!
    spark.sql(query).time()

    val profiles = run.awaitResults(60 * 1000L)
    println(Console.GREEN + s"Profile Link: ${profiles(0)}" + Console.RESET)
  }
}
