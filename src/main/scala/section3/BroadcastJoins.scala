package section3

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object BroadcastJoins {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 3.3 - Broadcast Joins")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  /**
    * Create Data Frames
    */
  val rows = sc.parallelize(List(
    Row(0, "zero"),
    Row(1, "first"),
    Row(2, "second"),
    Row(3, "third"),
  ))
  val rowsSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("order", StringType),
  ))

  val lookupTable = spark.createDataFrame(rows, rowsSchema)
  val largeTable = spark.range(1, 100000000)

  /**
    * Naive Join
    *
    * - Took 6 seconds on my computer
    * - 179.9 MiB	Shuffle in Stage 14 for the largeTable
    * - Both Data Frames are repartitioned, exchanged and sorted before the Join
    */
  val naiveJoin = largeTable.join(lookupTable, "id")
  //  naiveJoin.explain()
  //  naiveJoin.show()

  /**
    * Broadcast Join (Better)
    *
    * - Took 29 milliseconds
    * - No shuffle: Smaller Data Frame is Broadcast (sent) to all executors
    * - All the joins are done in memory with tiny overhead
    * - No need to repartition or exchange Data Frames
    * - Good for joining large Data Frames with small ones
    */
  val broadcastJoin = largeTable.join(
    broadcast(lookupTable),
    "id"
  )
  //  broadcastJoin.explain()
  //  broadcastJoin.show()

  /**
    * Auto-Broadcast Detection
    *
    * - Spark automatically optimizes for Broadcasts Joins
    * by estimating the size of the tables
    */
  val bigTable = spark.range(1, 1000000000)
  val smallTable = spark.range(1, 10000)
  val joinedNumbers = bigTable.join(smallTable, "id")
  val joinedNumbersV2 = smallTable.join(bigTable, "id")

  /**
    * We can configure the Broadcast threshold on the fly:
    * - Set the threshold to 30 bytes and it won't optimize for tables bigger than that.
    * - Set the threshold to -1 bytes and it won't optimize Broadcasts at all.
    */
  //    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 30)
  //    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  joinedNumbers.explain()
  joinedNumbersV2.explain()

  /**
    * We should only Broadcast SMALL Data Frames!!
    */

  def main(args: Array[String]): Unit = {
    /**
      * Keep the program running so we can analyze the webview
      */
    Thread.sleep(1000000)
  }
}
