package section2

import org.apache.spark.sql.{SaveMode, SparkSession}

object TestDeploy {


  /**
    * Receives Input and Output Files
    */
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("ERROR: Need an input file and an output file.")
      System.exit(1)
    }
    val moviesPath = args(0)
    val outputPath = args(1)

    /**
      * Boilerplate
      */
    val spark = SparkSession.builder()
      .appName("Lesson 2.6 - Test Deploy")
      // We can configure the Spark Session in the builder
      //      .config("spark.executor.memory", "1G")
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")


    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(moviesPath)

    val goodComedies = moviesDF.select(
      'Title as "title",
      'IMDB_Rating as "rating",
      'Release_Date as "release_date",
    )
      .where('Major_Genre === "Comedy" and 'IMDB_Rating > 6.5)
      .orderBy('rating.desc_nulls_last)


    // Or in the middle of the code for some parameters
    // spark.conf.set("spark.executor.memory", "1G")
    // WARNING: Can't configure executor memory in the middle of an application

    goodComedies.show()
    goodComedies.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(outputPath)

  }
}
