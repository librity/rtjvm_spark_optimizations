package section3

import common.readJsonDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import section3.JoinsRecap.spark

object ColumnPruning {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 3.4 - Column Pruning")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Setup
    */
  val bands = readJsonDF(spark, "bands")
  val guitarists = readJsonDF(spark, "guitarPlayers")
  val guitars = readJsonDF(spark, "guitars")

  val joinCondition = guitarists.col("band") === bands.col("id")

  /**
    * - Spark automatically does a Broadcast Join (Good)
    * - Spark does unnecessary Projects (.select("*"))  (Bad)
    */
  val guitaristsBands = guitarists.join(bands, joinCondition, "inner")
  //  guitaristsBands.explain()

  /*
  == Physical Plan ==
  *(2) BroadcastHashJoin [band#22L], [id#8L], Inner, BuildLeft
  :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#34]
  :  +- *(1) Project [band#22L, guitars#23, id#24L, name#25] TODO <-- UNCECESSARY (BAD)
  :     +- *(1) Filter isnotnull(band#22L)
  :        +- FileScan json [band#22L,guitars#23,id#24L,name#25] Batched: false, DataFilters: [isnotnull(band#22L)], Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/src/main/resources/data/guit..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,guitars:array<bigint>,id:bigint,name:string>
  +- *(2) Project [hometown#7, id#8L, name#9, year#10L] TODO <-- UNCECESSARY (BAD)
     +- *(2) Filter isnotnull(id#8L)
        +- FileScan json [hometown#7,id#8L,name#9,year#10L] Batched: false, DataFilters: [isnotnull(id#8L)], Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/src/main/resources/data/band..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<hometown:string,id:bigint,name:string,year:bigint>
   */


  /**
    * - Spark automatically does a Broadcast Join (Good)
    * - Spark does an optimal Projects (band.select("id")) (Good)
    * - By selecting the id column we exchange less data in the Broadcast
    */
  val guitaristsWithoutBands = guitarists.join(bands, joinCondition, "left_anti")
  //  guitaristsWithoutBands.explain()

  /*
  == Physical Plan ==
  *(2) BroadcastHashJoin [band#22L], [id#8L], LeftAnti, BuildRight
  :- FileScan json [band#22L,guitars#23,id#24L,name#25] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/src/main/resources/data/guit..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<band:bigint,guitars:array<bigint>,id:bigint,name:string>
  +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#63]
     +- *(1) Project [id#8L] TODO <-- COLUMN PRUNING (GOOD)
        +- *(1) Filter isnotnull(id#8L)
           +- FileScan json [id#8L] Batched: false, DataFilters: [isnotnull(id#8L)], Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/src/main/resources/data/band..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>
   */


  /**
    * Column Pruning
    *
    * - Remove unnecessary columns
    * - Shrinks Data Frame size
    * - Useful for Joins and Groups
    */


  /**
    * Project and Filter Push-down
    *
    * - Spark tends to drop columns as early as possible
    * - This should be YOUR goal as well:
    * Less columns -> smaller partitions -> less expensive Exchanges, Shuffles, etc.
    * - Prunes Columns before the Join
    * - Selects the columns that will be used by the Transformations and Actions
    */
  val names = guitaristsBands.select(
    guitarists.col("name"),
    bands.col("name"),
  )
  //  names.explain()

  /*
  == Physical Plan ==
  *(2) Project [name#25, name#9]
  +- *(2) BroadcastHashJoin [band#22L], [id#8L], Inner, BuildRight
     :- *(2) Project [band#22L, name#25] TODO <-- COLUMN PRUNING
     :  +- *(2) Filter isnotnull(band#22L)
     :     +- FileScan json [band#22L,name#25] Batched: false, DataFilters: [isnotnull(band#22L)], Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/src/main/resources/data/guit..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,name:string>
     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#42]
        +- *(1) Project [id#8L, name#9] TODO <-- COLUMN PRUNING
           +- *(1) Filter isnotnull(id#8L)
              +- FileScan json [id#8L,name#9] Batched: false, DataFilters: [isnotnull(id#8L)], Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/src/main/resources/data/band..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,name:string>
   */

  /**
    * Optimizing Map-side operation
    *
    * - Like "upper", " * 5", etc.
    */
  val rock = guitarists
    .join(bands, joinCondition)
    .join(
      guitars,
      array_contains(
        guitarists.col("guitars"),
        guitars.col("id"),
      )
    )
  val essentials = rock
    .select(
      guitarists.col("name"),
      bands.col("name"),
      upper(guitars.col("make")),
    )
  //  essentials.explain()

  /*
  == Physical Plan ==
  *(3) Project [name#25, name#9, upper(make#39) AS upper(make)#147] TODO <-- Final Project is done last (BAD)
  +- BroadcastNestedLoopJoin BuildRight, Inner, array_contains(guitars#23, id#38L)
     :- *(2) Project [guitars#23, name#25, name#9] TODO <-- COLUMN PRUNING (GOOD)
     :  +- *(2) BroadcastHashJoin [band#22L], [id#8L], Inner, BuildRight
     :     :- *(2) Project [band#22L, guitars#23, name#25] TODO <-- COLUMN PRUNING (GOOD)
     :     :  +- *(2) Filter isnotnull(band#22L)
     :     :     +- FileScan json [band#22L,guitars#23,name#25] Batched: false, DataFilters: [isnotnull(band#22L)], Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/src/main/resources/data/guit..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,guitars:array<bigint>,name:string>
     :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#58]
     :        +- *(1) Project [id#8L, name#9] TODO <-- COLUMN PRUNING (GOOD)
     :           +- *(1) Filter isnotnull(id#8L)
     :              +- FileScan json [id#8L,name#9] Batched: false, DataFilters: [isnotnull(id#8L)], Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/src/main/resources/data/band..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,name:string>
     +- BroadcastExchange IdentityBroadcastMode, [id=#48]
        +- FileScan json [id#38L,make#39] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/src/main/resources/data/guit..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:bigint,make:string>
   */

  /**
    * - If the joined Data Frame is much larger than
    * the one where we're applying the Map-side Function upper(),
    * it's better to do the upper() before the join:
    * We will run upper() far less times on a smaller Data Frame.
    * - Particularly useful for Outer Joins
    */
  val prunedGuitars = guitars.select($"id", upper($"make") as "make")
  val rockV2 = guitarists
    .join(bands, joinCondition)
    .join(
      prunedGuitars,
      array_contains(
        guitarists.col("guitars"),
        prunedGuitars.col("id"),
      )
    )
  val optimizedEssentials = rockV2
    .select(
      guitarists.col("name"),
      bands.col("name"),
      prunedGuitars.col("make"),
    )
  optimizedEssentials.explain()
  //  optimizedEssentials.show()

  /*
  == Physical Plan ==
  *(4) Project [name#25, name#9, make#151]
  +- BroadcastNestedLoopJoin BuildRight, Inner, array_contains(guitars#23, id#38L)
     :- *(2) Project [guitars#23, name#25, name#9] TODO <-- SPARK'S COLUMN PRUNING (GOOD)
     :  +- *(2) BroadcastHashJoin [band#22L], [id#8L], Inner, BuildRight
     :     :- *(2) Project [band#22L, guitars#23, name#25] TODO <-- SPARK'S COLUMN PRUNING (GOOD)
     :     :  +- *(2) Filter isnotnull(band#22L)
     :     :     +- FileScan json [band#22L,guitars#23,name#25] Batched: false, DataFilters: [isnotnull(band#22L)], Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/src/main/resources/data/guit..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,guitars:array<bigint>,name:string>
     :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#60]
     :        +- *(1) Project [id#8L, name#9] TODO <-- SPARK'S COLUMN PRUNING (GOOD)
     :           +- *(1) Filter isnotnull(id#8L)
     :              +- FileScan json [id#8L,name#9] Batched: false, DataFilters: [isnotnull(id#8L)], Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/src/main/resources/data/band..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,name:string>
     +- BroadcastExchange IdentityBroadcastMode, [id=#68]
        +- *(3) Project [id#38L, upper(make#39) AS make#151] TODO <-- MY COLUMN PRUNING (GOOD)
           +- FileScan json [id#38L,make#39] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_optimizations/src/main/resources/data/guit..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:bigint,make:string>
   */

  /**
    * - If you know that the joined table will be much smaller,
    * do the upper() after the join.
    */


  /**
    * Conclusion:
    *
    * - Spark selects just the relevant columns after a join
    * - If you do a select after a join, the Project operation is pushed to joined DFs
    * - Further map-side operations can be manually pushed down
    *   - if we anticipate the joined DF is bigger than either side
    *
    * - Spark sometimes can't prune columns automatically
    *   - good practice: select just the right columns ourselves before join
    *
    * - Most benefits seen in massive datasets
    * - Analyze Physical Plans and be mindful of Optimal/Unnecessary Projections
    */

  def main(args: Array[String]): Unit = {

  }
}
