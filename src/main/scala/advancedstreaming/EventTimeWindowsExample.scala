package advancedstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, sum, window}
import common.purchasesSchema

import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object EventTimeWindowsExample extends App {
  /**
   * Exercises
   * 1) Show the best selling product of every day, + quantity sold
   * 2) Show the best selling product of every 24 hours, update every hour. // add slideDuration 1 hour
   * */

  val spark = SparkSession.builder()
    .appName("Event Time Windows Exercise")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def betterSellingProductFromSocket() = {
    val purchasesDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), purchasesSchema).as("purchases"))
      .select("purchases.*") // this will show up all column in the schema

    // similar, we can read it from file

    val bestProductPurchasesDF = purchasesDF
      .groupBy(
        window(timeColumn = col("time"), windowDuration = "1 day").as("day"),
        col("item"))
      .agg(
        sum(col("quantity")).as("totalQuantity"))
      .orderBy(col("totalQuantity").desc_nulls_last)
      .select(
        col("day").getField("start").as("start"),
        col("day").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      )

    bestProductPurchasesDF.writeStream
      .format("console")
      .outputMode("complete") // Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;
      .trigger(
        Trigger.ProcessingTime(2.seconds)
        // 2 seconds from first data -> process
        // Continuous processing does not support Aggregate operations.
      )
      .start()
      .awaitTermination()
  }

  betterSellingProductFromSocket()

}
