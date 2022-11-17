package section5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import common.{buildJsonPath, inspect, readJsonDF}

object ByKeyFunctions {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 5.2 - .byKey() Functions")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  def main(args: Array[String]): Unit = {
    /**
      * Copy src/main/resources/data/lipsum/words.txt to spark-cluster/data/words.txt
      * and run spark-shell in the Docker Cluster
      */

    import scala.io.Source
    val words = Source.fromFile("/opt/spark-data/words.txt").getLines().toSeq

    import scala.util.Random
    val random = new Random

    /**
      * Generate random word frequency (simulating a scrape + .map())
      */
    val wordCounts = sc.parallelize(
      Seq.fill(2000000)(
        (
          words(random.nextInt(words.length)),
          random.nextInt(1000),
        )
      ))


    /**
      * .groupByKey()
      *
      * - Naive Way of reducing word count
      * - Took 8 seconds with an 11.0 MiB	Shuffle (BAD)
      * - Shuffles the ENTIRE data
      * - Can OutOfMemory crash your executors with skewed data (which can be unpredictable)
      */
    val totalCountsGBK = wordCounts.
      groupByKey().
      mapValues(_.sum)
    totalCountsGBK.collectAsMap()


    /**
      * .reduceByKey()
      *
      * - Took 3 seconds with a 7.8 KiB	Shuffle (BETTER)
      * - Does a partial reduction on every executor
      * - Avoids data skew problem
      * - Shuffles much less data
      */
    val totalCountsRBK = wordCounts.
      reduceByKey(_ + _)
    totalCountsRBK.collectAsMap()


    /**
      * .foldByKey()
      *
      * - Like .reduceByKey() but with an initial value
      * - Took 3 seconds with a 7.8 KiB	Shuffle (BETTER)
      */
    val totalCountsFBK = wordCounts.
      foldByKey(0)(_ + _)
    totalCountsFBK.collectAsMap()


    /**
      * .aggregateByKey()
      *
      * - More flexible/powerful but with a more complex API
      * - Took 3 seconds with a 7.8 KiB	Shuffle (BETTER)
      * - Can be dangerous, especially if used with expansion
      * - Always use reductions
      */
    val totalCountsABK = wordCounts.
      aggregateByKey(0)(
        // (a, b) => a + b,
        _ + _,
        _ + _,
      )
    totalCountsABK.collectAsMap()


    /**
      * .combineByKey()
      *
      * - Took 3 seconds with a 7.8 KiB	Shuffle (BETTER)
      * - Most powerful ByKey() API we can use
      * - All other functions call this in the source
      * - numPartitions = 2: The resulting RDD will have 2 partitions
      * - partitioner = somePartitioner: Use a custom partitioner
      * - Can be dangerous, especially if used with expansion
      * - Always use reductions
      */
    val totalCountsCBK = wordCounts.
      combineByKey(
        (count: Int) => count,
        (currentSum: Int, newValue: Int) => currentSum + newValue,
        (partialSum1: Int, partialSum2: Int) => partialSum1 + partialSum2,
        numPartitions = 2,
        // partitioner = somePartitioner
      )
    totalCountsCBK.collectAsMap()


  }
}
