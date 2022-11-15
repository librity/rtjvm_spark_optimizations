package section3

import common.{inspect, readJsonDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object JoinsRecap {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 3.2 - Joins Recap")
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

  val joinCondition = guitarists.col("band") === bands.col("id")

  /**
    * Inner Join
    */
  val guitaristBandsInner = guitarists.join(bands, joinCondition, "inner")

  /**
    * Left Outer Join
    *
    * - Inner Join + rows of the left table with null values
    */
  val guitaristBandsLeftOuter = guitarists.join(bands, joinCondition, "left_outer")

  /**
    * Right Outer Join
    *
    * - Inner Join + rows of the right table with null values
    */
  val guitaristBandsRightOuter = guitarists.join(bands, joinCondition, "right_outer")

  /**
    * Full Outer Join
    *
    * - Inner Join + Left Outer + Right Outer
    */
  val guitaristBandsFullOuter = guitarists.join(bands, joinCondition, "outer")


  /**
    * Left Semi Join
    *
    * - Everything in the left Data Frame that satisfies the condition
    * - Like a .select() or .filter()
    */
  val guitaristBandsSemi = guitarists.join(bands, joinCondition, "left_semi")

  /**
    * Left Anti Join
    *
    * - Everything in the left Data Frame that does not satisfy the condition
    */
  val guitaristBandsAnti = guitarists.join(bands, joinCondition, "left_anti")


  /**
    * Cross Join
    *
    * - Basically the cartesian product
    * - Everything in the left table with everything in the right
    */
  val guitaristBandsCross = guitarists.join(
    bands,
    guitarists.col("band") === guitarists.col("band"),
    "cross"
  )


  /**
    * RDD Joins
    *
    * - We can join RDDs of Tuples
    */
  val colorScores = Seq(
    ("blue", 1),
    ("red", 4),
    ("green", 5),
    ("yellow", 2),
    ("orange", 3),
    ("cyan", 0),
  )
  val colorsRDD: RDD[(String, Int)] = sc.parallelize(colorScores)

  /**
    * Count word occurrence with map/reduce
    *
    * .reduceByKey(): The key is the first element of the tuple
    */
  val text = "The sky is blue but the orange pale sun turns from yellow to red"
  val wordsMap = text.split(" ").map(word => (word.toLowerCase(), 1))
  val wordsRDD = sc.parallelize(wordsMap).reduceByKey(_ + _)

  /**
    * - Inner Join between these RDDs by the first element of the tuple (implied)
    * - The join type is specified by the method:
    * .join() (Inner), .fullOuterJoin(), .leftOuterJoin() and .rightOuterJoin()
    */
  val scores: RDD[(String, (Int, Int))] = wordsRDD.join(colorsRDD)

  /**
    *
    */

  /**
    *
    */

  /**
    *
    */

  def main(args: Array[String]): Unit = {
    //        inspect(guitars)
    //        inspect(bands)
    //        inspect(guitarists)

    //        inspect(guitaristBandsInner)
    //
    //        inspect(guitaristBandsLeftOuter)
    //        inspect(guitaristBandsRightOuter)
    //        inspect(guitaristBandsFullOuter)
    //
    //        inspect(guitaristBandsSemi)
    //        inspect(guitaristBandsAnti)
    inspect(guitaristBandsCross)

    //    wordsMap.foreach(println)
    //    wordsRDD.foreach(println)
    //    scores.foreach(println)
  }
}