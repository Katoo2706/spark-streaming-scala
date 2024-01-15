package structuredstreaming

import org.apache.spark.sql.{Dataset, DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import common.{Cars, carsSchema}


object StreamingDatasets extends App {
  val spark = SparkSession.builder()
    .appName("Streaming datasets")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  def readCars(): Dataset[Cars] = {

    // covert to dataset
    val carEncoder = Encoders.product[Cars]

    // Read data from key-value object (dictionary)
    val streamedCars = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with a single string column "value"
      .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // this will show up all column in the schema
      .as[Cars] // encoder can be passed implicitly with spark.implicits (carEncoder)

    streamedCars
  }

  def showCarNames() = {
    val carsDS: Dataset[Cars] = readCars()

    // transformations here
    val carNamesDF: DataFrame = carsDS.select(col("Name")) // Dataframe

    // Collection transformations maintain type info
    val carNamesAlt: Dataset[String] = carsDS.map(_.Name) // Dont need Encoders[String] with implicits

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /** Exercises
   *
   * 1. Count how many Powerful cars we have in the DS (HP > 140)
   * 2. Average HP for the entire dataset
   *    (Use the complete output mode)
   * 3. Count the cars by origin
   * */

  def exercise_1() = {
    val carsDS: Dataset[Cars] = readCars()

    carsDS.filter(_.Horsepower.getOrElse(0L) > 140) // Long
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }



  showCarNames()
}
