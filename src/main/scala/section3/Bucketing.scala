package section3

import common.readJsonDF
import org.apache.spark.sql.SparkSession

object Bucketing {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 3.6 - Bucketing")
    .master("local[*]")
    // Deactivate Broadcast Joins
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Typical Join
    */
  val large = spark.range(1, 1000000)
    .selectExpr("id * 5 AS id")
    .repartition(10)
  val small = spark.range(1, 10000)
    .selectExpr("id * 3 AS id")
    .repartition(3)
  val joined = large.join(small, "id")

  //  joined.explain()
  /*
  == Physical Plan ==
  *(5) Project [id#2L]
  +- *(5) SortMergeJoin [id#2L], [id#6L], Inner
     :- *(2) Sort [id#2L ASC NULLS FIRST], false, 0
     :  +- Exchange hashpartitioning(id#2L, 200), true, [id=#40]
     :     +- Exchange RoundRobinPartitioning(10), false, [id=#39]
     :        +- *(1) Project [(id#0L * 5) AS id#2L]
     :           +- *(1) Range (1, 1000000, step=1, splits=8)
     +- *(4) Sort [id#6L ASC NULLS FIRST], false, 0
        +- Exchange hashpartitioning(id#6L, 200), true, [id=#47]
           +- Exchange RoundRobinPartitioning(3), false, [id=#46]
              +- *(3) Project [(id#4L * 3) AS id#6L]
                 +- *(3) Range (1, 10000, step=1, splits=8)
   */

  /**
    * Bucketing
    *
    * - Good for on multiple joins/groups
    * - .bucketBy(4, "id"): Saves Data Frame as 4 File System buckets (files)
    * with the rows with the same hash for "id" staying in the same bucket.
    * - Saving to this is almost as expensive as a Shuffle
    * - Once loaded it's as fast as a pre-partitioning,
    * and subsequent joins will be really fast (no shuffles)
    * - Tables are saved as .parquet
    * - Like a checkpoint: one time cost for repeated use improvement
    */
  large.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_large")

  small.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_small")

  val bucketedLarge = spark.table("bucketed_large")
  val bucketedSmall = spark.table("bucketed_small")

  val bucketedJoin = bucketedLarge.join(bucketedSmall, "id")
  bucketedJoin.explain()
  /*
  == Physical Plan ==
  *(3) Project [id#11L]
  +- *(3) SortMergeJoin [id#11L], [id#13L], Inner
     :- *(1) Sort [id#11L ASC NULLS FIRST], false, 0
     :  +- *(1) Project [id#11L]
     :     +- *(1) Filter isnotnull(id#11L)
     :        +- *(1) ColumnarToRow
     :           +- FileScan parquet default.bucketed_large[id#11L] Batched: true, DataFilters: [isnotnull(id#11L)], Format: Parquet, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/spark-warehouse/bucketed_large], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4
     +- *(2) Sort [id#13L ASC NULLS FIRST], false, 0
        +- *(2) Project [id#13L]
           +- *(2) Filter isnotnull(id#13L)
              +- *(2) ColumnarToRow
                 +- FileScan parquet default.bucketed_small[id#13L] Batched: true, DataFilters: [isnotnull(id#13L)], Format: Parquet, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/spark-warehouse/bucketed_small], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4
  */


  /**
    * - joined.count(): 4-5 seconds
    * - bucketedJoin.count(): 4 seconds for bucketing, 0.5 seconds for counting
    */
  //  joined.count()
  //  bucketedJoin.count()


  /**
    * Bucketing for Groups
    */
  val flights = readJsonDF(spark, "flights")
    .repartition(2)

  val mostDelayed = flights
    .filter("origin = 'DEN' AND arrdelay > 1")
    .groupBy("origin", "dest", "carrier")
    .avg("arrdelay")
    .orderBy($"avg(arrdelay)".desc_nulls_last)

  //  mostDelayed.explain()
  /*
  == Physical Plan ==
  *(4) Sort [avg(arrdelay)#60 DESC NULLS LAST], true, 0
  +- Exchange rangepartitioning(avg(arrdelay)#60 DESC NULLS LAST, 200), true, [id=#80] TODO <-- SHUFFLE (BAD)
     +- *(3) HashAggregate(keys=[origin#34, dest#31, carrier#25], functions=[avg(arrdelay#24)])
        +- Exchange hashpartitioning(origin#34, dest#31, carrier#25, 200), true, [id=#76] TODO <-- SHUFFLE (BAD)
           +- *(2) HashAggregate(keys=[origin#34, dest#31, carrier#25], functions=[partial_avg(arrdelay#24)])
              +- Exchange RoundRobinPartitioning(2), false, [id=#72] TODO <-- OUR REPARTITIONING
                 +- *(1) Project [arrdelay#24, carrier#25, dest#31, origin#34] TODO <-- COLUMN PRUNING
                    +- *(1) Filter (((isnotnull(origin#34) AND isnotnull(arrdelay#24)) AND (origin#34 = DEN)) AND (arrdelay#24 > 1.0)) TODO <-- SPARK FILTER OPTIMIZATION
                       +- FileScan json [arrdelay#24,carrier#25,dest#31,origin#34] Batched: false, DataFilters: [isnotnull(origin#34), isnotnull(arrdelay#24), (origin#34 = DEN), (arrdelay#24 > 1.0)], Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/src/main/resources/data/flig..., PartitionFilters: [], PushedFilters: [IsNotNull(origin), IsNotNull(arrdelay), EqualTo(origin,DEN), GreaterThan(arrdelay,1.0)], ReadSchema: struct<arrdelay:double,carrier:string,dest:string,origin:string>
  */


  /**
    * - Takes just as long as the two Shuffles
    * - Subsequent grouping will be much faster
    */
  //  flights.write
  //    .partitionBy("origin")
  //    .bucketBy(4, "dest", "carrier")
  //    .saveAsTable("bucketed_flights")
  //
  //  val bucketedFlights = spark.table("bucketed_flights")
  //  val bucketedMostDelayed = bucketedFlights
  //    .filter("origin = 'DEN' AND arrdelay > 1")
  //    .groupBy("origin", "dest", "carrier")
  //    .avg("arrdelay")
  //    .orderBy($"avg(arrdelay)".desc_nulls_last)
  //
  //  bucketedMostDelayed.explain()
  /*
  == Physical Plan ==
  *(2) Sort [avg(arrdelay)#144 DESC NULLS LAST], true, 0
  +- Exchange rangepartitioning(avg(arrdelay)#144 DESC NULLS LAST, 200), true, [id=#53]
     +- *(1) HashAggregate(keys=[origin#118, dest#115, carrier#109], functions=[avg(arrdelay#108)]) TODO <-- NO SHUFFLE (GOOD)
        +- *(1) HashAggregate(keys=[origin#118, dest#115, carrier#109], functions=[partial_avg(arrdelay#108)]) TODO <-- NO SHUFFLE (GOOD)
           +- *(1) Project [arrdelay#108, carrier#109, dest#115, origin#118]
              +- *(1) Filter (isnotnull(arrdelay#108) AND (arrdelay#108 > 1.0))
                 +- *(1) ColumnarToRow
                    +- FileScan parquet default.bucketed_flights[arrdelay#108,carrier#109,dest#115,origin#118] Batched: true, DataFilters: [isnotnull(arrdelay#108), (arrdelay#108 > 1.0)], Format: Parquet, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/spark-warehouse/bucketed_fli..., PartitionFilters: [isnotnull(origin#118), (origin#118 = DEN)], PushedFilters: [IsNotNull(arrdelay), GreaterThan(arrdelay,1.0)], ReadSchema: struct<arrdelay:double,carrier:string,dest:string>, SelectedBucketsCount: 4 out of 4
  */


  /**
    * - mostDelayed.show(): 1 second for a JSON smaller than 1 MB (BAD)
    * - bucketedMostDelayed.show(): 0.2 seconds, 5* the performance (GOOD)
    */
  //  mostDelayed.show()
  //  bucketedMostDelayed.show()


  /**
    * Bucket Pruning
    *
    * - SelectedBucketsCount: 1 out of 4
    * - Only reads the file that contains the item we're filtering
    * - Spark automatically does this
    * - the10.show() runs very fast
    */
  val the10 = bucketedLarge.filter($"id" === 10)
  the10.explain()
  /*
  == Physical Plan ==
  *(1) Project [id#11L]
  +- *(1) Filter (isnotnull(id#11L) AND (id#11L = 10))
     +- *(1) ColumnarToRow
        +- FileScan parquet default.bucketed_large[id#11L] Batched: true, DataFilters: [isnotnull(id#11L), (id#11L = 10)], Format: Parquet, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/spark-warehouse/bucketed_large], PartitionFilters: [], PushedFilters: [IsNotNull(id), EqualTo(id,10)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 1 out of 4 TODO <-- BUCKET PRUNING (GOOD)
  */

  the10.show()


  def main(args: Array[String]): Unit = {

    /**
      * Delete the spark-warehouse directory before running.
      */

  }
}
