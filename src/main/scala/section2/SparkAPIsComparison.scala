package section2

import org.apache.spark.sql.SparkSession

object SparkAPIsComparison {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 2.5 - Spark APIs Comparison")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    /**
      * Run Spark Shell in the cluster's master node and open the Web UI
      */

    /**
      * RDDs
      */
    val rdd = sc.parallelize(1 to 1000000000)
    rdd.count()


    /**
      * Data Frames
      *
      * - Slower than RDD for this operation
      * - Converting RDD to Data Frame is very expensive:
      * The entire RDD is Scanned and serialized,
      * then partitions are partially and totally counted
      */
    import spark.implicits._
    val df = rdd.toDF("id")
    df.count()
    df.selectExpr("count(*)").show()
    df.explain(true)


    /**
      * Data Sets
      *
      * - Faster than RDDs and Data Frames for this operation
      * - Treated like a Scala range: doesn't evaluate all the elements
      */
    val ds = spark.range(1, 1000000000)
    ds.count()
    ds.selectExpr("count(*)").show()


    /**
      * Conclusion: Pick the right API for your Queries/Transformations
      */


    val timesFiveRdd = rdd.map(_ * 5)
    timesFiveRdd.count()

    /**
      * These take about the same:
      *
      * - The "* 5" happens in the projection after the Scan and Serialization
      */
    val timesFiveDF = df.selectExpr("id * 5 AS id")
    timesFiveDF.count()

    df.explain()
    timesFiveDF.explain()


    /**
      * These also take the same time:
      *
      * - The physical plans for these are identical:
      * Spark eliminates the unnecessary project and just serializes and counts
      */
    val dfCount = df.selectExpr("count(*)")
    val timesFiveDFCount = timesFiveDF.selectExpr("count(*)")

    dfCount.explain()
    timesFiveDFCount.explain()


    /**
      * Slower than range Data Set:
      *
      * - Need to evaluate range for the map,
      * which also requires to deserialize and serialize it
      * - Functional transformations aren't optimized by Spark,
      * they run exactly as you call them
      */
    val dsCount = ds.selectExpr("count(*)")
    val timesFiveDS = ds.map(_ * 5)
    val timesFiveDSCount = timesFiveDS.selectExpr("count(*)")

    dsCount.show()
    timesFiveDSCount.show()

    dsCount.explain()
    timesFiveDSCount.explain()


    /**
      * Conclusions:
      *
      * - Once you decide on an API level, stay there
      * - Use Data Frames most of the time (Spark optimizes many things for you)
      * - Lambdas wreck Dataset performance
      */
  }
}
