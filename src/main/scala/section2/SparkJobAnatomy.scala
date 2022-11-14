package section2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import common.{buildJsonPath, inspect, readJsonDF}

object SparkJobAnatomy {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 2.3 - Spark Job Anatomy")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    /**
      * Run Spark Shell in the cluster's master node
      */
    val rdd1 = sc.parallelize(1 to 1000000)

    rdd1.count()
    rdd1.map(_ * 2).count()
    rdd1.repartition(23).count()


    /**
      * Common Aggregation
      *
      * - Narrow transformations: isolated on each partition,
      * like .map(), .mapValues(), .select() and projections
      * - Wide transformations: All partitions need to be considered,
      * and there might even be some shuffling, like .groupByKey(), .join() and .orderBy()
      * - Generally, the less stages the better the performance
      */
    // Stage 1
    val employees = sc.textFile("/tmp/data/employees/employees.csv")
    val employeeTokens = employees.map(_.split(","))
    val employeeDetails = employeeTokens.map(tokens => (tokens(4), tokens(7)))

    // Stage 2
    val employeeGroups = employeeDetails.groupByKey(2)
    val averageSalaries = employeeGroups.mapValues(salaries => salaries.map(_.toInt).sum / salaries.size)
    averageSalaries.collect().foreach(println)


    /**
      * Optimized with single map
      */
    // Stage 1
    val employeeDetailsV2 = employees.map { line =>
      val tokens = line.split(",")
      (tokens(4), tokens(7))
    }

    // Stage 2
    val employeeGroupsV2 = employeeDetailsV2.groupByKey(2)
    val averageSalariesV2 = employeeGroupsV2.mapValues(salaries => salaries.map(_.toInt).sum / salaries.size)
    averageSalariesV2.collect().foreach(println)


  }
}
