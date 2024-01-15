package advancedstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, expr, length, sum, window}


object ProcessingTimeWindows extends App {
  val spark = SparkSession.builder()
    .appName("Processing Time Windows")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def aggregateProcessingTime() = {
    val linesDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(col("value"), current_timestamp().as("processingTime"))

    val aggregateLinesDF = linesDF.groupBy(
        window(col("processingTime"), "10 seconds").as("window"),
        col("value"))
      .agg(sum(length(col("value"))).as(
        "length")) // counting characters every 10 seconds by processing time
      .select(
        expr("window.*"), // struct field = "start", "end"
        col("length"),
        col("value")
      )

    aggregateLinesDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  aggregateProcessingTime()

}
