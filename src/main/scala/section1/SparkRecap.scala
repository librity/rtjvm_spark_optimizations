package section1

import common.{buildJsonPath, inspect, readJsonDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkRecap {
  /**
    * Boilerplate
    */

  val spark = SparkSession.builder()
    .appName("Lesson 1.2 - Spark Recap")
    // Run locally on 2 threads
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  /**
    * Read Data Frames
    */

  val cars = readJsonDF(spark, "cars")
  val guitars = readJsonDF(spark, "guitars")
  val guitarists = readJsonDF(spark, "guitarPlayers")

  /**
    * Define Data Sets
    */
  case class
  Guitarist(
             id: Long,
             name: String,
             guitars: Seq[Long],
             band: Long,
           )

  val guitaristDS = guitarists.as[Guitarist]


  def main(args: Array[String]): Unit = {
    //    inspect(cars)
    //    inspect(guitars)
    //    inspect(guitarists)


    /**
      * Select
      */
    val carWeights = cars
      .select(
        col("Name"),
        $"Year",
        'Weight_in_lbs.as("weight_in_lbs"),
        (col("weight_in_lbs") / 2.2) as "weight_in_kg",
        expr("weight_in_lbs / 2.2 AS weight_in_kg_v2"),
      )
    //    carWeights.show()

    val carWeightsKg = carWeights
      .selectExpr("weight_in_lbs / 2.2 AS weight_in_kg")
    //    carWeightsKg.show()


    /**
      * Filter
      */
    val europeanCars = cars
      .filter(cars.col("Origin") =!= "USA")
    //    europeanCars.show()


    /**
      * Aggregations:
      *
      * sum, min, max, mean, avg, stddev, etc.
      */
    val averageHP = cars
      .agg(
        avg("Horsepower").as("average_hp")
      )
    //    averageHP.show()


    /**
      * Grouping
      */
    val countByOrigin = cars
      .groupBy($"Origin")
      .count()
    //    countByOrigin.show()


    /**
      * Joining
      */
    val joinExample = guitarists
      .join(
        guitars,
        array_contains(guitarists.col("guitars"), guitars.col("id"))
      )
    //    joinExample.show()

    /**
      * Data Sets
      */

    val allGuitarists = guitaristDS
      .map(_.name)
      .reduce(_ + ", " + _)
    //    println(s"Guitarists: $allGuitarists")

    /**
      * Spark SQL
      */
    cars.createOrReplaceTempView("cars")

    val americanCars = spark.sql(
      """
        |SELECT Name
        |    FROM cars
        |    WHERE
        |        Origin = 'USA'
        |""".stripMargin)
    //    americanCars.show()

    /**
      * Low-Level API: RDDs
      *
      * Use functional operators.
      */
    val numbersRDD: RDD[Int] = sc.parallelize(1 to 1000000)
    val evensRDD = numbersRDD.map(_ * 2)


    /**
      * RDD to Data Frame
      *
      * Lose type info, gain SQL capabilities.
      */
    val evensDF = evensRDD.toDF("evens")


    /**
      * RDD to Data Set
      *
      * Keeps type information AND SQL capabilities.
      */
    val evensDS = spark.createDataset(evensRDD)


    /**
      * Data Set to RDD
      */
    val guitaristRDD = guitaristDS.rdd


    /**
      * Data Frame to RDD
      *
      * RDD[Row]
      */
    val carsRDD = cars.rdd

  }
}
