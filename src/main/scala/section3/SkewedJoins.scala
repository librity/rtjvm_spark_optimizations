package section3

import generator.DataGenerator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SkewedJoins {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 3.7 - Skewed Joins")
    .master("local[*]")
    // Deactivate Broadcast Joins
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * An online store selling gaming laptops:
    * - 2 Laptops are "similar" if they have the same make & model, and procSpeed within 0.1
    * - For each laptop configuration, we are interested in the average sale price of "similar" models
    * Example:
    * Acer Predator 2.9Ghz d asd aisfj0iw -> Average sale price of All Acer Predators with CPU speed between 2.8 and 3.0 Ghz
    */

  /**
    * Generate Laptops
    */
  val laptops = Seq
    .fill(40000)(DataGenerator.randomLaptop())
    .toDS()
  val laptopOffers = Seq
    .fill(100000)(DataGenerator.randomLaptopOffer())
    .toDS()

  val joined = laptops
    .join(laptopOffers, Seq("make", "model"))
    .filter(
      abs(
        laptopOffers.col("procSpeed") - laptops.col("procSpeed")
      ) <= 1.0
    )
    .groupBy("registration")
    .agg(avg("salePrice") as "averageSalePrice")


  //  joined.explain()
  /*
  == Physical Plan ==
  *(4) HashAggregate(keys=[registration#4], functions=[avg(salePrice#20)])
  +- Exchange hashpartitioning(registration#4, 200), true, [id=#45]
     +- *(3) HashAggregate(keys=[registration#4], functions=[partial_avg(salePrice#20)])
        +- *(3) Project [registration#4, salePrice#20]
           +- *(3) SortMergeJoin [make#5, model#6], [make#17, model#18], Inner, (abs((procSpeed#19 - procSpeed#7)) <= 1.0)
              :- *(1) Sort [make#5 ASC NULLS FIRST, model#6 ASC NULLS FIRST], false, 0
              :  +- Exchange hashpartitioning(make#5, model#6, 200), true, [id=#23]
              :     +- LocalTableScan [registration#4, make#5, model#6, procSpeed#7]
              +- *(2) Sort [make#17 ASC NULLS FIRST, model#18 ASC NULLS FIRST], false, 0
                 +- Exchange hashpartitioning(make#17, model#18, 200), true, [id=#24]
                    +- LocalTableScan [make#17, model#18, procSpeed#19, salePrice#20]
  */


  /**
    * - 2 minutes for a megabyte Data Set (Very Slow)
    * - The poor performance isn't explainable through the query plan!
    * - Small shuffle (~ 1 MB)
    * - Straggling Tasks in Stage 2
    * - The data is skewed: 50% of the generated data has the same make and model
    * - Because of the .join() 50% of the data stays in the same executor
    * - The join is giving the executor a disproportional amount of data in that task
    */
  //  joined.show()


  /**
    * Handling Skewed Data
    *
    * - Many ways to handle this
    * - Distribute data as evenly as possible
    * - We can do this in this situation since the procSpeed only has 2 digits
    * - explode() creates more rows with a distribution of values that will cancel each other out in the agg(avg())
    * - Join by procSpeed too since it's less skewed than make and model
    */

  val deskewedLaptops = laptops
    .withColumn("procSpeed",
      explode(array(
        $"procSpeed" - 0.1, $"procSpeed", $"procSpeed" + 0.1
      ))
    )
  //  deskewedLaptops.show()

  val optimizedJoin = deskewedLaptops
    .join(laptopOffers, Seq("make", "model", "procSpeed"))
    .groupBy("registration")
    .agg(avg("salePrice") as "averageSalePrice")

  optimizedJoin.explain()
  /*
  == Physical Plan ==
  *(4) HashAggregate(keys=[registration#4], functions=[avg(salePrice#20)])
  +- Exchange hashpartitioning(registration#4, 200), true, [id=#49]
     +- *(3) HashAggregate(keys=[registration#4], functions=[partial_avg(salePrice#20)])
        +- *(3) Project [registration#4, salePrice#20]
           +- *(3) SortMergeJoin [make#5, model#6, knownfloatingpointnormalized(normalizenanandzero(procSpeed#43))], [make#17, model#18, knownfloatingpointnormalized(normalizenanandzero(procSpeed#19))], Inner
              :- *(1) Sort [make#5 ASC NULLS FIRST, model#6 ASC NULLS FIRST, knownfloatingpointnormalized(normalizenanandzero(procSpeed#43)) ASC NULLS FIRST], false, 0
              :  +- Exchange hashpartitioning(make#5, model#6, knownfloatingpointnormalized(normalizenanandzero(procSpeed#43)), 200), true, [id=#27]
              :     +- Generate explode(array((procSpeed#7 - 0.1), procSpeed#7, (procSpeed#7 + 0.1))), [registration#4, make#5, model#6], false, [procSpeed#43]
              :        +- LocalTableScan [registration#4, make#5, model#6, procSpeed#7]
              +- *(2) Sort [make#17 ASC NULLS FIRST, model#18 ASC NULLS FIRST, knownfloatingpointnormalized(normalizenanandzero(procSpeed#19)) ASC NULLS FIRST], false, 0
                 +- Exchange hashpartitioning(make#17, model#18, knownfloatingpointnormalized(normalizenanandzero(procSpeed#19)), 200), true, [id=#28]
                    +- LocalTableScan [make#17, model#18, procSpeed#19, salePrice#20]

   */
  /**
    * - 18 seconds, much better
    * - Less skewed, more parallelism
    * - The more distributed your cluster, the more of a performance impact this will have
    * - Physical Plan doesn't explain the performance increase
    */
  optimizedJoin.show()


  /**
    * Solution:
    *
    * - include extra information in the join key set
    * - new key to join with
    * - redistribution of data by n+1 join keys
    * - uniform tasks
    */


  def main(args: Array[String]): Unit = {


    Thread.sleep(1000000)
  }
}
