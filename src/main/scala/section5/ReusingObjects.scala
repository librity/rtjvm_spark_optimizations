package section5

import generator.DataGenerator.generateText
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReusingObjects {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 5.3 - Reusing JVM Objects")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Goal: Analyze Text
    *
    * - Receive batches of text from known data sources: "SOURCE_ID // SOME_TEXT"
    * - Statistics for each data SOURCE_ID:
    *   - Total lines
    *   - Total words
    *   - Length of longest word
    *   - Number of occurrences of the word "imperdiet"
    *
    * - Results should be very fast (like in a Stream)
    * - We can't do 4 different passes, that would be too slow
    */


  val textDataPath = "src/main/resources/generated/lipsum/3m.txt"

  def generateTextData() = {
    generateText(textDataPath, 70000000, 3000000, 200)
  }

  val text = sc.textFile(textDataPath)
    .map { line =>
      val tokens = line.split(" // ")

      (tokens(0).toInt, tokens(1))
    }

  val targetWord = "imperdit"


  /**
    * Naive Collect
    *
    * - Took 7 seconds with a 113.2 KiB Shuffle
    * - Not bad, but could be better
    * - We end up instantiating thousands of TextStats JVM objects
    * - Garbage collection adds up
    */

  case class TextStats(lineCount: Int, wordCount: Int, longestWordLength: Int, targetCount: Int)

  object TextStats {
    val initial = TextStats(0, 0, 0, 0)
  }


  def collectStats() = {

    def aggregateNewRecord(stats: TextStats, line: String): TextStats = {
      val words = line.split(" ")
      //      val longestWord = words.reduce((a, b) => if (a > b) a else b).size
      val longestWord = words.maxBy(_.length)
      //      val newOccurrences = words.filter(_ == targetWord).size
      val newOccurrences = words.count(_ == targetWord)

      TextStats(
        stats.lineCount + 1,
        stats.wordCount + words.length,
        if (longestWord.length > stats.longestWordLength) longestWord.length else stats.longestWordLength,
        stats.targetCount + newOccurrences,
      )
    }

    def combineStats(stats1: TextStats, stats2: TextStats): TextStats = {
      TextStats(
        stats1.lineCount + stats2.lineCount,
        stats1.wordCount + stats2.wordCount,
        //        if (stats1.longestWordLength > stats2.longestWordLength) stats1.longestWordLength else stats2.longestWordLength,
        Math.max(stats1.longestWordLength, stats2.longestWordLength),
        stats1.targetCount + stats2.targetCount,
      )
    }

    val aggregate: RDD[(Int, TextStats)] = text
      .aggregateByKey(TextStats.initial)(
        aggregateNewRecord,
        combineStats,
      )

    aggregate.collectAsMap()
  }


  /**
    * With Mutable Data Structures
    *
    * - Functional Heresy but more performatic
    * - Took 6 seconds with a 114.9 KiB	Shuffle
    * - Slightly more performatic
    * - By using mutable structures we instantiate less JVM objects
    * and save on memory and garbage collection
    * - Must extend Serializable since these instances will be shuffled around the cluster
    */
  case class
  MutableTextStats(
                    var lineCount: Int,
                    var wordCount: Int,
                    var longestWordLength: Int,
                    var targetCount: Int
                  ) extends Serializable

  object MutableTextStats extends Serializable {
    val initial = MutableTextStats(0, 0, 0, 0)
  }

  def collectMutableStats() = {
    def aggregateNewRecord(stats: MutableTextStats, line: String): MutableTextStats = {
      val words = line.split(" ")
      val longestWord = words.maxBy(_.length)
      val newOccurrences = words.count(_ == targetWord)


      stats.lineCount += 1
      stats.wordCount += words.length
      stats.longestWordLength = if (longestWord.length > stats.longestWordLength) longestWord.length else stats.longestWordLength
      stats.targetCount += newOccurrences

      stats
    }

    def combineStats(stats1: MutableTextStats, stats2: MutableTextStats): MutableTextStats = {
      stats1.lineCount += stats2.lineCount
      stats1.wordCount += stats2.wordCount
      stats1.longestWordLength = Math.max(stats1.longestWordLength, stats2.longestWordLength)
      stats1.targetCount += stats2.targetCount

      stats1
    }

    val aggregate: RDD[(Int, MutableTextStats)] = text
      .aggregateByKey(MutableTextStats.initial)(
        aggregateNewRecord,
        combineStats,
      )

    aggregate.collectAsMap()
  }


  /**
    * With JVM Arrays
    *
    * - Even lighter than Mutable Case Classes
    * - Took 5 seconds with a 46.0 KiB Shuffle
    * - Performance scales with Data Set size
    * - When we call .maxBy() and .count() on words it converts it to an indexed Seq (BAD)
    * - Need to make code serializable across the cluster with SerializableTextStats
    */
  object SerializableTextStats extends Serializable {
    val lineCount = 0
    val wordCount = 1
    val longestWordLength = 2
    val targetCount = 3

    def aggregateNewRecord(stats: Array[Int], line: String): Array[Int] = {
      val words: Array[String] = line.split(" ")


      var i = 0
      while (i < words.length) {
        val word = words(i)
        val wordLength = word.length


        stats(longestWordLength) = if (wordLength > stats(longestWordLength)) wordLength else stats(longestWordLength)
        stats(targetCount) += (if (word == targetWord) 1 else 0)


        i += 1
      }


      stats(lineCount) += 1
      stats(wordCount) += words.length

      stats
    }

    def combineStats(stats1: Array[Int], stats2: Array[Int]): Array[Int] = {
      stats1(lineCount) += stats2(lineCount)
      stats1(wordCount) += stats2(wordCount)
      //      stats1(longestWordLength) = Math.max(stats1(longestWordLength), stats2(longestWordLength))
      stats1(longestWordLength) = if (stats1(longestWordLength) > stats2(longestWordLength)) stats1(longestWordLength) else stats2(longestWordLength)
      stats1(targetCount) += stats2(targetCount)

      stats1
    }

  }

  def collectArrayStats() = {

    //    val initialArray = Array(0, 0, 0, 0)
    val initialArray = Array.fill(4)(0)


    val aggregate: RDD[(Int, Array[Int])] = text
      .aggregateByKey(initialArray)(
        SerializableTextStats.aggregateNewRecord,
        SerializableTextStats.combineStats,
      )

    aggregate.collectAsMap()
  }


  def main(args: Array[String]): Unit = {
    /**
      * Generate Data (do this once)
      */
    //    generateTextData()


    //    collectStats()
    //    collectMutableStats()
    collectArrayStats()

    Thread.sleep(1000000)
  }
}
