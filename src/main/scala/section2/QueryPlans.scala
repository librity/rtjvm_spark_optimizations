package section2

import org.apache.spark.sql.SparkSession

object QueryPlans {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 2.1 - Query Plans")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    /**
      * Run Spark Shell in the cluster's master node
      */

    /**
      * Read query plans with .explain()
      */
    val simpleNumbers = spark.range(1, 1000000)
    val timesFive = simpleNumbers.selectExpr("id * 5 AS id")

    timesFive.explain(true)


    val moreNumbers = spark.range(1, 1000000, 2)
    val splitSeven = moreNumbers.repartition(7)

    splitSeven.explain(true)
    splitSeven.selectExpr("id * 5 AS id").explain(true)


    /**
      * Derived Data Frames
      *
      * - .join() Needs to repartition and sort before joining (very expensive):
      *     - Rows with the same key need to be on the same partition
      *     - Spark picks a common arbitrary number of partitions for the RDDs
      *
      * - sum() Does a partial sum for each partition, then adds the partials together
      */
    val ds1 = spark.range(1, 10000000)
    val ds2 = spark.range(1, 20000000, 2)
    val ds3 = ds1.repartition(7)
    val ds4 = ds2.repartition(9)
    val ds5 = ds3.selectExpr("id * 3 AS id")

    val joined = ds5.join(ds4, "id")
    val sum = joined.selectExpr("sum(id)")

    sum.explain(true)


  }
}
