package section4

import generator.{DataGenerator, Laptop, LaptopOffer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{abs, avg}

object SkewedRDDJoins {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 4.4 - Skewed RDD Joins")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Same problem of section3.SkewedJoins but with RDDs
    * An online store selling gaming laptops:
    * - 2 Laptops are "similar" if they have the same make & model, and procSpeed within 0.1
    * - For each laptop configuration, we are interested in the average sale price of "similar" models
    * Example:
    * Acer Predator 2.9Ghz d asd aisfj0iw -> Average sale price of All Acer Predators with CPU speed between 2.8 and 3.0 Ghz
    */


  /**
    * Generate RDDs
    */
  val laptops = sc.parallelize(
    Seq.fill(40000)(DataGenerator.randomLaptop())
  )
  val laptopOffers = sc.parallelize(
    Seq.fill(100000)(DataGenerator.randomLaptopOffer())
  )


  /**
    * Naive Join
    *
    * - 2.2 minutes for a megabyte Data Set (Very Slow)
    * - Straggling Tasks in Stage 2 (2.1 minutes)
    * - The data is skewed: 50% of the generated data has the same make and model
    * - Because of the .join() 50% of the data stays in the same executor
    * - The join is giving the executor a disproportional amount of data in that task
    */
  def naiveJoin() = {
    /**
      * Create RDDs of Tuples with key = (make, model)
      */
    val preparedLaptops = laptops.map {
      case Laptop(registration, make, model, procSpeed) =>
        ((make, model), (registration, procSpeed))
    }
    val preparedOffers = laptopOffers.map {
      case LaptopOffer(make, model, procSpeed, salePrice) =>
        ((make, model), (procSpeed, salePrice))
    }

    /**
      * .join() => RDD[((make, model), ((registration, procSpeed), (procSpeed, salePrice)))]
      */
    val result = preparedLaptops.join(preparedOffers)
      .filter {
        case (
          (make, model), ((registration, laptopSpeed), (offerSpeed, salePrice))
          ) => Math.abs(laptopSpeed - offerSpeed) <= 0.1
      }
      .map {
        case ((_, _), ((registration, _), (_, salePrice))) => (registration, salePrice)
      }

      /**
        * With Data Frames: .groupBy(registration).avg(salePrice)
        *
        * We could do: .groupByKey().mapValues(prices => prices.sum / prices.size)
        * - Bad idea, very slow
        *
        * .aggregateByKey(initialValue)(partialAggregator(), finalAggregator()) => RDD[(registration, (totalPrice, count))]
        * - Control aggregation by partition
        * - partialAggregator(): Aggregation in the partition, combine state with record
        * - finalAggregator(): Aggregation between partitions, combine 2 states into one
        * - Much faster
        */
      .aggregateByKey((0.0, 0))(
        {
          case ((totalPrice, priceCount), salePrice) =>
            (totalPrice + salePrice, priceCount + 1)
        },
        {
          case ((totalPrice1, priceCount1), (totalPrice2, priceCount2)) =>
            (totalPrice1 + totalPrice2, priceCount1 + priceCount2)
        },
      )
      .mapValues {
        case (totalPrice, count) => totalPrice / count
      }

    result
      .take(10)
      .foreach(println)
  }


  /**
    * De-skew Data before joining
    *
    * - Time: 43 seconds!
    * - 6 Straggling task that took more than 20 seconds
    * - Distribute data as evenly as possible
    * - We can do this in this situation since the procSpeed only has 2 digits
    * - Join by procSpeed too since it's less skewed than make and model
    * - .flatMap() creates more rows with a distribution of values
    * that will cancel each other out in the .aggregateByKey()()
    */
  def deskewedJoin() = {
    /**
      * Create RDDs of Tuples with key = (make, model)
      */
    val preparedLaptops = laptops.flatMap {
      case Laptop(registration, make, model, procSpeed) =>
        Seq(
          ((make, model, procSpeed - 0.1), registration),
          ((make, model, procSpeed), registration),
          ((make, model, procSpeed + 0.1), registration),
        )
    }
    val preparedOffers = laptopOffers.map {
      case LaptopOffer(make, model, procSpeed, salePrice) =>
        ((make, model, procSpeed), salePrice)
    }

    val result = preparedLaptops.join(preparedOffers)
      .map(_._2)
      .aggregateByKey((0.0, 0))(
        {
          case ((totalPrice, priceCount), salePrice) =>
            (totalPrice + salePrice, priceCount + 1)
        },
        {
          case ((totalPrice1, priceCount1), (totalPrice2, priceCount2)) =>
            (totalPrice1 + totalPrice2, priceCount1 + priceCount2)
        },
      )
      .mapValues {
        case (totalPrice, count) => totalPrice / count
      }


    result
      .take(10)
      .foreach(println)
  }


  /**
    * Warning: Straggling Tasks can crash Executors!
    */

  def main(args: Array[String]): Unit = {
    naiveJoin()
    deskewedJoin()

    Thread.sleep(1000000)
  }
}
