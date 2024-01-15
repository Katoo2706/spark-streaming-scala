package advancedstreaming

import org.apache.spark.sql.functions.{col, from_json, sum, window}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object EventTimeWindows extends App {
  val spark = SparkSession.builder()
    .appName("Event Time Windows")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  // Create schema
  val onlinePurchaseSchema = StructType(Array(
      StructField("id", StringType),
      StructField("time", TimestampType),
      StructField("item", StringType),
      StructField("quantity", IntegerType),
    ))

  def readPurchasesFromSocket() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
    .selectExpr("purchase.*")

  def aggregatePurchaseBySlidingWindow()= {
    val purchasesDF = readPurchasesFromSocket()

    /*
    * Slide window:
    * - windowDuration:  1 day > aggregate by 1 day from first record (eg: 2019-02-17 03:00:00 - 2019-02-18 03:00:00)
    * - slideDuration: 1 hour => interval 1 hour from start time
    *
    * => Have 24 different records for a single batch.
    * Return all the windows that contain the record time
    *
    * Tumbing windows:
    * - Fixed-sized, non-overlapping (without slide Duration parameters)
    *
    *
    * */
    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day").as("time")) // struct column: has fields {start, end}
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  aggregatePurchaseBySlidingWindow()


}
