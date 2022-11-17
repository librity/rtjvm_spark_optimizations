package section4

import org.apache.spark.sql.SparkSession

object BroadcastRDDJoins {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 4.3 - Broadcast RDD Joins")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    /**
      * Run this in Docker Cluster's Spark Shell
      */

    import scala.util.Random
    val random = new Random()

    val awards = sc.parallelize(List((1, "gold medal"), (2, "silver medal"), (3, "silver medal")))
    val leaderboard = sc.parallelize(1 to 10000000).map((_, random.alphanumeric.take(8).mkString))

    /**
      * Naive Join
      *
      * - 37 seconds to Join an small RDD with a big RDD
      */
    val naiveWinners = leaderboard.join(awards)
    naiveWinners.foreach(println)


    /**
      * Broadcast Join
      *
      * 1. Collect RDD back to the driver/master process
      * 2. Broadcast Map variable to all the executors
      * 3. Map partitions and records with the broadcasted variable
      *
      * - We can only use broadcastedAwards in the partitions because we broadcasted it
      * - Job now takes 3 seconds
      */
    val broadcastedAwards = awards.collectAsMap()
    sc.broadcast(broadcastedAwards)
    val improvedWinners = leaderboard.mapPartitions { partition =>
      partition.flatMap { record =>
        val (position, name) = record
        broadcastedAwards.get(position) match {
          case None => Seq.empty
          case Some(award) => Seq((name, award))
        }
      }
    }
    improvedWinners.foreach(println)


  }
}
