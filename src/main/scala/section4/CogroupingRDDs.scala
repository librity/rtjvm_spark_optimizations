package section4

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CogroupingRDDs {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 4.2 - Cogrouping RDDs")
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

  def readIds() = sc
    .textFile(s"$examDataDir/examIds.txt")
    .map { line =>
      val tokens = line.split(" ")

      (tokens(0).toLong, tokens(1))
    }

  def readScores() = sc
    .textFile(s"$examDataDir/examScores.txt")
    .map { line =>
      val tokens = line.split(" ")

      (tokens(0).toLong, tokens(1).toDouble)
    }

  def readEmails() = sc
    .textFile(s"$examDataDir/examEmails.txt")
    .map { line =>
      val tokens = line.split(" ")

      (tokens(0).toLong, tokens(1))
    }


  /**
    * Goals
    *
    * - IF a student passed (at least 1 attempt score >= 9.0)
    * THEN send him an email with "PASSED"
    * - ELSE send him an email with "FAILED"
    */


  /**
    * Naive Solution
    *
    * - Straight forward
    * - Three joins and a map
    * - Time: 10 seconds
    */
  def naiveCount() = {
    val scores = readScores().reduceByKey(Math.max)
    val candidates = readIds()
    val emails = readEmails()

    val result = candidates
      .join(scores) // RDD[(id, (name, maxScore))]
      .join(emails) // RDD[(id, ((name, maxScore), email)]
      .mapValues {
        case ((_, maxScore), email) =>
          if (maxScore >= 9.0) (email, "PASSED")
          else (email, "FAILED")
      }

    result.count()
  }


  /**
    * Co-grouped Join
    *
    * - .cogroup(): Copartition the three RDDs
    * - RDD[(Long, (Iterable[String], Iterable[Double], Iterable[String]))]
    * - RDD[(id, (names[], scores[], emails[]))]
    * - Iterable will only have one item (in this case)
    * - Time: 7 seconds
    * - The seconds result.count() runs even faster since the RDDs have been repartitioned (1 second).
    * Skips Stages 8, 9 and 10!
    */
  def coGroupedCount() = {
    val scores = readScores().reduceByKey(Math.max)
    val candidates = readIds()
    val emails = readEmails()

    val result: RDD[(Long, Option[(String, String)])] = candidates
      .cogroup(scores, emails)
      .mapValues {
        case (_, maxScoreIterable, emailIterable) =>
          val maxScore = maxScoreIterable.headOption
          val email = emailIterable.headOption

          for {
            e <- email
            s <- maxScore
          } yield (e, if (s >= 9.0) "PASSED" else "FAILED")
      }

    result.count()
    result.count()
  }


  /**
    * Conclusions
    *
    * - .cogroup() makes sure ALL the RDDs share the same partitioner
    * - Particularly useful for multi-way joins
    *   - all RDDs are shuffled at most once
    *   - RDDs are never shuffled again if cogrouped RDD is reused
    *
    * - Keeps the entire data - equivalent to a full outer join
    *   - for each key, an iterator of values is given
    *   - if there is no value for a "column", the respective iterator is empt
    */


  def main(args: Array[String]): Unit = {
    naiveCount()
    coGroupedCount()


    Thread.sleep(1000000)
  }
}
