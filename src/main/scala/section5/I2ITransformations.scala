package section5

import generator.DataGenerator.generateMetrics
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object I2ITransformations {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 5.5 - Iterator to Iterator (I2I) Transformations")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

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
    * Naive Solution
    *
    * - .sortBy() took 3 seconds with no Shuffle
    * - .take() took 11 seconds with a 333.7 MiB Shuffle
    * - Really bad for a simple sort and limit
    */
  def naiveBestMetrics() = {
    val bestMetrics = readMetrics()
      //      .sortBy(tuple => tuple._2)
      .sortBy(_._2)
      .take(LIMIT)

    bestMetrics.foreach(println)
  }


  /**
    * With Iterator to Iterator (I2I)
    *
    * - Took 4 seconds with a 6.6 KiB	Shuffle
    * - Performance scales with Data Set size
    *
    * - .mapPartitions(): Callback receives and returns an Iterator
    * - Narrow transformations (no Shuffles!)
    * - Avoids OutOfMemory errors by spilling overload to disk
    * - Warning: We can only traverse the Iterator once
    * - Warning: Converting Iterator to Collection (List, Seq, etc.)
    * loads the entire partition to memory and kills performance
    *
    * - Strategy: get the 10 best metrics of each partition,
    * then the 10 best metrics of those
    * - Much better than sorting the entire thing
    *
    * - TreeSet: (Implicitly) ordered collection
    * - .repartition(1): Bring all the values to a single partition
    */
  def i2iBestMetrics() = {


    val getBestPartitionMetrics = (records: Iterator[(String, Double)]) => {
      implicit val ordering: Ordering[(String, Double)] =
        Ordering.by[(String, Double), Double](_._2)
      val limitedCollection = new mutable.TreeSet[(String, Double)]()

      records.foreach { record =>
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
    //    naiveBestMetrics()
    i2iBestMetrics()


    Thread.sleep(1000000)
  }
}
