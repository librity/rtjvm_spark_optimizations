package section5

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object I2ITransformationsExercise {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 5.6 - Iterator to Iterator (I2I) Transformations Exercise")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Science Project
    *
    * - Each metric has an identifier and value
    * - Return the smallest ("best") 10 metrics with identifier and value
    */


  /**
    * Generate Metrics (do this once)
    */
  val metricsDataPath = "src/main/resources/generated/metrics/metrics10m.txt"
  //  generateMetrics(metricsDataPath, 10000000)


  /**
    * Load Metrics as RDD
    */
  def readMetrics() = {
    sc.textFile(metricsDataPath)
      .map { line =>
        val tokens = line.split(" ")
        val name = tokens(0)
        val value = tokens(1).toDouble

        (name, value)
      }
  }

  val LIMIT = 10


  /**
    * Exercise 1 - Is this a good solution?
    *
    * - Bad mainly because of .toList: Transforming to collection
    * loads the entire partition to memory and kills performance.
    * This can also crash executors
    * - Besides that the .sortBy() will be narrow so it wouldn't be super bad
    * - 12 seconds with a 6.6 KiB Shuffle
    * - Better than naiveBestMetrics, worse than i2iBestMetrics
    */
  def printBestMetricsEx1() = {
    val bestMetrics = readMetrics()
      .mapPartitions(
        _.toList.sortBy(_._2).take(LIMIT).toIterator
      )
      .repartition(1)
      .mapPartitions(
        _.toList.sortBy(_._2).take(LIMIT).toIterator
      )
      .take(LIMIT)

    bestMetrics.foreach(println)
  }


  /**
    * Exercise 2 - Is this a good solution?
    *
    * - Bad mainly because of .toList:
    *   - Transforming to collection loads the entire partition to memory and kills performance
    *   - We end up iterating twice
    *   - This can also crash executors (OOM)
    *   - If the list is immutable, we waste time allocating objects and with Garbage Collection
    *
    * - Took 9 seconds with a 6.6 KiB	Shuffle
    * - Better than printBestMetricsEx1, still worse than i2iBestMetrics
    */
  def printBestMetricsEx2() = {


    val getBestPartitionMetrics = (records: Iterator[(String, Double)]) => {
      implicit val ordering: Ordering[(String, Double)] =
        Ordering.by[(String, Double), Double](_._2)
      val limitedCollection = new mutable.TreeSet[(String, Double)]()

      records.toList.foreach { record =>
        limitedCollection.add(record)

        if (limitedCollection.size > LIMIT) {
          limitedCollection.remove(limitedCollection.last)
        }
      }

      limitedCollection.toIterator
    }


    val bestMetrics = readMetrics()
      .mapPartitions(getBestPartitionMetrics)
      .repartition(1)
      .mapPartitions(getBestPartitionMetrics)
      .take(LIMIT)


    bestMetrics.foreach(println)
  }


  def main(args: Array[String]): Unit = {
    //    printBestMetricsEx1()
    printBestMetricsEx2()


    Thread.sleep(1000000)
  }
}
