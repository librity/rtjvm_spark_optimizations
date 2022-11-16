package section3

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object PrePartitioning {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 3.5 - Pre Partitioning")
    .master("local[*]")
    // Deactivate Broadcast Joins
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  /**
    * - `+:` Prepends "id" to newColumns
    * - `: _*` Pass collection as a Variadic Arguments
    *
    * @param dataFrame The Data Frame to be added
    * @param n         The number of new columns
    * @tparam T The type of the Data Frame
    * @return The a DataFrame with the new columns.
    *         addColumns(initialTable, 3) => Dataframe("id", "newCol1", "newCol2", "newCol3")
    */
  def addColumns[T](dataFrame: Dataset[T], n: Int): DataFrame = {
    val newColumns = (1 to n)
      .map(i => s"id * $i AS newCol$i")

    dataFrame.selectExpr(("id" +: newColumns): _*)
  }

  /**
    * Don't touch this
    */
  val initialTable = spark.range(1, 10000000).repartition(10) // RoundRobinPartitioning(10)
  val narrowTable = spark.range(1, 5000000).repartition(7) // RoundRobinPartitioning(7)


  /**
    * Scenario 1
    *
    * - Slower than Scenario 2
    * - .repartition(n): Repartition by n partitions uses the RoundRobinPartitioner:
    * Creates n equally-sized partition with no regard for order.
    */
  val wideTable = addColumns(initialTable, 30)
  val joined1 = wideTable.join(narrowTable, "id")

  //  joined1.explain()
  //  println(joined1.count())


  /*
  == Physical Plan ==
  *(6) Project [id#0L, newCol1#8L, newCol2#9L, newCol3#10L, newCol4#11L, newCol5#12L, newCol6#13L, newCol7#14L, newCol8#15L, newCol9#16L, newCol10#17L, newCol11#18L, newCol12#19L, newCol13#20L, newCol14#21L, newCol15#22L, newCol16#23L, newCol17#24L, newCol18#25L, newCol19#26L, newCol20#27L, newCol21#28L, newCol22#29L, newCol23#30L, ... 7 more fields]
  +- *(6) SortMergeJoin [id#0L], [id#4L], Inner
     :- *(3) Sort [id#0L ASC NULLS FIRST], false, 0
     :  +- Exchange hashpartitioning(id#0L, 200), true, [id=#39]
     :     +- *(2) Project [id#0L, (id#0L * 1) AS newCol1#8L, (id#0L * 2) AS newCol2#9L, (id#0L * 3) AS newCol3#10L, (id#0L * 4) AS newCol4#11L, (id#0L * 5) AS newCol5#12L, (id#0L * 6) AS newCol6#13L, (id#0L * 7) AS newCol7#14L, (id#0L * 8) AS newCol8#15L, (id#0L * 9) AS newCol9#16L, (id#0L * 10) AS newCol10#17L, (id#0L * 11) AS newCol11#18L, (id#0L * 12) AS newCol12#19L, (id#0L * 13) AS newCol13#20L, (id#0L * 14) AS newCol14#21L, (id#0L * 15) AS newCol15#22L, (id#0L * 16) AS newCol16#23L, (id#0L * 17) AS newCol17#24L, (id#0L * 18) AS newCol18#25L, (id#0L * 19) AS newCol19#26L, (id#0L * 20) AS newCol20#27L, (id#0L * 21) AS newCol21#28L, (id#0L * 22) AS newCol22#29L, (id#0L * 23) AS newCol23#30L, ... 7 more fields]
     :        +- Exchange RoundRobinPartitioning(10), false, [id=#35]
     :           +- *(1) Range (1, 10000000, step=1, splits=8)
     +- *(5) Sort [id#4L ASC NULLS FIRST], false, 0
        +- Exchange hashpartitioning(id#4L, 200), true, [id=#46]
           +- Exchange RoundRobinPartitioning(7), false, [id=#45]
              +- *(4) Range (1, 5000000, step=1, splits=8)
   */

  /**
    * Scenario 2
    *
    * - Faster than Scenario 1
    * - Joins with co-partitioned RDDs
    * - Less total repartitions
    * - Spark calls the latest .repartition() so to not partition twice.
    * - .repartition($"id"): Repartitioning by a column uses the HashPartitioner:
    * Guarantees that the rows with the same value stay in the same partition
    * - addColumns() Projection occurs on the final (smaller) table
    */
  val altNarrow = narrowTable.repartition($"id")
  val altInitial = initialTable.repartition($"id")
  val joined2 = altInitial.join(altNarrow, "id")
  val result2 = addColumns(joined2, 30)

  //  result2.explain()
  //  println(result2.count())


  /*
  == Physical Plan ==
  *(5) Project [id#0L, (id#0L * 1) AS newCol1#105L, (id#0L * 2) AS newCol2#106L, (id#0L * 3) AS newCol3#107L, (id#0L * 4) AS newCol4#108L, (id#0L * 5) AS newCol5#109L, (id#0L * 6) AS newCol6#110L, (id#0L * 7) AS newCol7#111L, (id#0L * 8) AS newCol8#112L, (id#0L * 9) AS newCol9#113L, (id#0L * 10) AS newCol10#114L, (id#0L * 11) AS newCol11#115L, (id#0L * 12) AS newCol12#116L, (id#0L * 13) AS newCol13#117L, (id#0L * 14) AS newCol14#118L, (id#0L * 15) AS newCol15#119L, (id#0L * 16) AS newCol16#120L, (id#0L * 17) AS newCol17#121L, (id#0L * 18) AS newCol18#122L, (id#0L * 19) AS newCol19#123L, (id#0L * 20) AS newCol20#124L, (id#0L * 21) AS newCol21#125L, (id#0L * 22) AS newCol22#126L, (id#0L * 23) AS newCol23#127L, ... 7 more fields]
  +- *(5) SortMergeJoin [id#0L], [id#4L], Inner
     :- *(2) Sort [id#0L ASC NULLS FIRST], false, 0
     :  +- Exchange hashpartitioning(id#0L, 200), false, [id=#91]
     :     +- *(1) Range (1, 10000000, step=1, splits=8)
     +- *(4) Sort [id#4L ASC NULLS FIRST], false, 0
        +- Exchange hashpartitioning(id#4L, 200), false, [id=#97]
           +- *(3) Range (1, 5000000, step=1, splits=8)
   */


  /**
    * Conclusion: Partition early!
    */


  /**
    * Scenario 3
    *
    * - Faster than Scenario 1 but slower than Scenario 2
    * - Performs an unnecessary repartition due to the addColumns()
    * - addColumns() also runs on a bigger table
    * - enhancedRepartitioned Is useless:
    * The join will perform an Exchange hashpartitioning
    */
  val addColumnsFirst = addColumns(initialTable, 30)
  val narrowRepartitioned = narrowTable.repartition($"id")
  val enhancedRepartitioned = addColumnsFirst.repartition($"id") // USELESS!
  val result3 = enhancedRepartitioned.join(narrowRepartitioned, "id")

  //  result3.explain()
  //  println(result3.count())


  /*
  == Physical Plan ==
    *(6) Project [id#0L, newCol1#166L, newCol2#167L, newCol3#168L, newCol4#169L, newCol5#170L, newCol6#171L, newCol7#172L, newCol8#173L, newCol9#174L, newCol10#175L, newCol11#176L, newCol12#177L, newCol13#178L, newCol14#179L, newCol15#180L, newCol16#181L, newCol17#182L, newCol18#183L, newCol19#184L, newCol20#185L, newCol21#186L, newCol22#187L, newCol23#188L, ... 7 more fields]
    +- *(6) SortMergeJoin [id#0L], [id#4L], Inner
       :- *(3) Sort [id#0L ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(id#0L, 200), false, [id=#41]
       :     +- *(2) Project [id#0L, (id#0L * 1) AS newCol1#166L, (id#0L * 2) AS newCol2#167L, (id#0L * 3) AS newCol3#168L, (id#0L * 4) AS newCol4#169L, (id#0L * 5) AS newCol5#170L, (id#0L * 6) AS newCol6#171L, (id#0L * 7) AS newCol7#172L, (id#0L * 8) AS newCol8#173L, (id#0L * 9) AS newCol9#174L, (id#0L * 10) AS newCol10#175L, (id#0L * 11) AS newCol11#176L, (id#0L * 12) AS newCol12#177L, (id#0L * 13) AS newCol13#178L, (id#0L * 14) AS newCol14#179L, (id#0L * 15) AS newCol15#180L, (id#0L * 16) AS newCol16#181L, (id#0L * 17) AS newCol17#182L, (id#0L * 18) AS newCol18#183L, (id#0L * 19) AS newCol19#184L, (id#0L * 20) AS newCol20#185L, (id#0L * 21) AS newCol21#186L, (id#0L * 22) AS newCol22#187L, (id#0L * 23) AS newCol23#188L, ... 7 more fields]
       :        +- Exchange RoundRobinPartitioning(10), false, [id=#37]
       :           +- *(1) Range (1, 10000000, step=1, splits=8)
       +- *(5) Sort [id#4L ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(id#4L, 200), false, [id=#47]
             +- *(4) Range (1, 5000000, step=1, splits=8)
  */


  /**
    * Conclusion: Partitioning late is AT BEST what Spark naturally does.
    */


  /**
    * Exercise: Would it be better if we repartitioned narrowTable with 10 instead of 7?
    *
    * - Terrible idea
    * - Equivalent to Scenario 1
    * - It would still have to repartition both tables before the join with hashpartitioning()
    * - Only hashpartitioning() can guarantee that all the rows with the same id are in the same partition
    */
  initialTable
    .join(
      narrowTable.repartition(10),
      "id"
    )
    .explain()


  /*
  == Physical Plan ==
  *(5) Project [id#0L]
  +- *(5) SortMergeJoin [id#0L], [id#4L], Inner
     :- *(2) Sort [id#0L ASC NULLS FIRST], false, 0
     :  +- Exchange hashpartitioning(id#0L, 200), true, [id=#32]
     :     +- Exchange RoundRobinPartitioning(10), false, [id=#31]
     :        +- *(1) Range (1, 10000000, step=1, splits=8)
     +- *(4) Sort [id#4L ASC NULLS FIRST], false, 0
        +- Exchange hashpartitioning(id#4L, 200), true, [id=#39]
           +- Exchange RoundRobinPartitioning(10), false, [id=#38]
              +- *(3) Range (1, 5000000, step=1, splits=8)
   */

  def main(args: Array[String]): Unit = {

  }
}
