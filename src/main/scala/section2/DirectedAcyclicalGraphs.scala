package section2

import org.apache.spark.sql.SparkSession

object DirectedAcyclicalGraphs {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 2.4 - Directed Acyclical Graphs (DAGs)")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    /**
      * Run Spark Shell in the cluster's master node and open the Web UI
      */

    /**
      * Analyze the DAG for the following computations:
      */
    sc.parallelize(1 to 1000000).count()

    val rdd1 = sc.parallelize(1 to 1000000)
    rdd1.map(_ * 2).count()
    rdd1.repartition(23).count()


    val ds1 = spark.range(1, 10000000)
    val ds2 = spark.range(1, 20000000, 2)
    val ds3 = ds1.repartition(7)
    val ds4 = ds2.repartition(9)
    val ds5 = ds3.selectExpr("id * 3 AS id")

    val joined = ds5.join(ds4, "id")
    val sum = joined.selectExpr("sum(id)")
    sum.show()
  }
}
