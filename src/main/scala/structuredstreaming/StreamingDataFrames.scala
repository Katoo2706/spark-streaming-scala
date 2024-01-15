package structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import common.stocksSchema

import scala.concurrent.duration._

object StreamingDataFrames extends App {
  val spark = SparkSession.builder()
    .appName("Structured Streaming")
    .config("spark.master", "local[2]") // 2 threads or local[*]
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  /**
   * Streaming from Socket*/
  def readFromSocket() = {
    // reading a DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // transformation (simple filter)
    val shortLines: DataFrame = lines.filter(length(col("value")) <= 5)

    // tell between a static vs a streaming DF
    println(shortLines.isStreaming)

    // start the streaming query
    // consume a DF and print to console (col: 'value')
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    // wait for the stream to finish
    query.awaitTermination()
  }

  /**
   * Streaming from File
   * - read: static read
   * - readStream: Add new data in streaming dataframe
   * - trigger: Read data when new data arrival
   * */

  def readFromFiles() = {
    val stocksDF = spark.readStream
      .format("csv")
      .option("header", false)
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination() // Waits for the termination of this query
  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // write lines DF as a certain trigger
    // fetch batch data retrieval in every 2 seconds (mini-batch)
    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
//        Trigger.ProcessingTime(2.seconds) // can use "2 seconds"
//        Trigger.AvailableNow() // Single batch, then terminate
        Trigger.Continuous(2.seconds) // every 2 seconds create a batch event data or not, support few operations & from kafka
        // Continuous processing does not support Aggregate operations.
        // https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#supported-queries
      )
      .start()
      .awaitTermination()
  }

  demoTriggers()

}
