package section4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import common.{buildJsonPath, inspect, readJsonDF}
import generator.DataGenerator.generateExamData
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

object SimpleRDDJoins {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 4.1 - Simple RDD Joins")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Generate Exam Data (do once)
    */
  val examDataDir = "src/main/resources/generated/exam_data"
  // generateExamData(examDataDir, 1000000, 5)


  /**
    * - HashPartitioner(10):
    *   - Ten partitions
    *   - Rows with the same key (hash) stay in the same partition;
    */
  def readIds() = sc
    .textFile(s"$examDataDir/examIds.txt")
    .map { line =>
      val tokens = line.split(" ")

      (tokens(0).toLong, tokens(1))
    }
    .partitionBy(new HashPartitioner(10))

  def readExamScores() = sc
    .textFile(s"$examDataDir/examScores.txt")
    .map { line =>
      val tokens = line.split(" ")

      (tokens(0).toLong, tokens(1).toDouble)
    }


  /**
    * Goals
    *
    * - Determine the number of students who passed the exam
    * (at least one score of > 9.0)
    */


  /**
    * Join First
    *
    * - Bad Performance
    * - joined: (id (score, name))
    * - .reduceByKey(): Get the max score of each candidate
    * - Time: 10 seconds
    */
  def simpleJoinCount() = {
    val candidates = readIds()
    val scores = readExamScores()

    val joined: RDD[(Long, (Double, String))] = scores.join(candidates)
    val finalScores = joined
      .reduceByKey { (a, b) =>
        if (a._1 > b._1) a else b
      }
      .filter { attempt =>
        val score = attempt._2._1

        score > 9.0
      }


    finalScores.count()
  }


  /**
    * Aggregate First:
    *
    * - Reduce early, smaller tables for the .join(), better performance
    * - Roughly 10% performance increase
    * - Time: 5 seconds
    */
  def preAggregateCount() = {
    val candidates = readIds()
    val scores = readExamScores()

    val maxScores: RDD[(Long, Double)] = scores
      .reduceByKey(Math.max)
    val finalScores = maxScores
      .join(candidates)
      .filter { attempt =>
        val score = attempt._2._1

        score > 9.0
      }

    finalScores.count()
  }


  /**
    * .reduceByKey() and .filter() before the .join()
    * - Time: 4 seconds
    */
  def preFilterCount() = {
    val candidates = readIds()
    val scores = readExamScores()

    val maxScores = scores
      .reduceByKey(Math.max)
      .filter { attempt =>
        val score = attempt._2

        score > 9.0
      }
    val finalScores = maxScores.join(candidates)

    finalScores.count()
  }


  /**
    * RDDs share the same partitioner
    * - Time: 3 seconds
    */
  def coPartitionCount() = {
    val candidates = readIds()
    val scores = readExamScores()

    val scoresPartitioner = candidates.partitioner match {
      case None => new HashPartitioner(candidates.getNumPartitions)
      case Some(partitioner) => partitioner
    }

    val repartitionedScores = scores.partitionBy(scoresPartitioner)


    val joined: RDD[(Long, (Double, String))] = repartitionedScores.join(candidates)
    val finalScores = joined
      .reduceByKey { (a, b) =>
        if (a._1 > b._1) a else b
      }
      .filter { attempt =>
        val score = attempt._2._1

        score > 9.0
      }


    finalScores.count()
  }


  /**
    * All optimizations
    *
    * - Time: < 3 seconds
    */
  def combinedCount() = {
    val candidates = readIds()
    val scores = readExamScores()

    val scoresPartitioner = candidates.partitioner match {
      case None => new HashPartitioner(candidates.getNumPartitions)
      case Some(partitioner) => partitioner
    }
    val repartitionedScores = scores.partitionBy(scoresPartitioner)


    val maxScores = repartitionedScores
      .reduceByKey(Math.max)
      .filter { attempt =>
        val score = attempt._2

        score > 9.0
      }
    val finalScores = maxScores.join(candidates)

    finalScores.count()
  }


  /**
    * Conclusions:
    *
    * - Spark won't optimize RDDs for us
    * - Try to combine as many optimizations as possible
    * - Experiment and measure performance
    */


  def main(args: Array[String]): Unit = {
    simpleJoinCount()
    preAggregateCount()
    preFilterCount()
    coPartitionCount()
    combinedCount()


    Thread.sleep(1000000)
  }
}
