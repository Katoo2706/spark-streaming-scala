package advancedstreaming

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.streaming.Trigger

import java.io.PrintStream
import scala.concurrent.duration.DurationInt

object Watermarks extends App {
  val spark = SparkSession.builder()
    .appName("Late Data with Watermarks")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  // 3000, blue
  // want to process data as string => Dataset
  def testWatermark() = {
    val dataDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        val timestamp = new Timestamp(tokens(0).toLong)
        val data = tokens(1)

        (timestamp, data)

      }.toDF("created", "color") // colNames


    // count records for each color every 2 seconds
    val watermarkedDF = dataDF
      .withWatermark("created", "2 seconds")
      .groupBy(
        window(col("created"), "2 seconds"),
        col("color"))
      .count()
      .selectExpr("window.*", "color", "count")

    /**
     * 2 seconds watermark means:
     * - a window will only be considered util the watermark surpasses the window end
     * - an element/a row/a record will be considered if AFTER the watermark*/

    val query = watermarkedDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()

    // in package object
    debugQuery(query)

    query.awaitTermination()
  }

  testWatermark()
  /* Debug output
    Batch: 1
    13: {min=1970-01-01T00:00:07.000Z, avg=1970-01-01T00:00:07.500Z, watermark=1970-01-01T00:00:00.000Z, max=1970-01-01T00:00:08.000Z}
    14: {min=1970-01-01T00:00:07.000Z, avg=1970-01-01T00:00:07.500Z, watermark=1970-01-01T00:00:00.000Z, max=1970-01-01T00:00:08.000Z}
    15: {min=1970-01-01T00:00:07.000Z, avg=1970-01-01T00:00:07.500Z, watermark=1970-01-01T00:00:00.000Z, max=1970-01-01T00:00:08.000Z}
    16: {min=1970-01-01T00:00:07.000Z, avg=1970-01-01T00:00:07.500Z, watermark=1970-01-01T00:00:00.000Z, max=1970-01-01T00:00:08.000Z}
    * ...

    Batch: 2
    17: {min=1970-01-01T00:00:14.000Z, avg=1970-01-01T00:00:14.000Z, watermark=1970-01-01T00:00:06.000Z, max=1970-01-01T00:00:14.000Z}
    18: {min=1970-01-01T00:00:14.000Z, avg=1970-01-01T00:00:14.000Z, watermark=1970-01-01T00:00:06.000Z, max=1970-01-01T00:00:14.000Z}
    19: {min=1970-01-01T00:00:14.000Z, avg=1970-01-01T00:00:14.000Z, watermark=1970-01-01T00:00:06.000Z, max=1970-01-01T00:00:14.000Z}

    => Watermark in batch 2 will be max event time in batch 1 (previous batch - 2 seconds
    watermark=1970-01-01T00:00:06.000Z
    => Records earlier than watermark=1970-01-01T00:00:06.000Z will not be processed in batch 2

    => after that... if printer.println("9000,red") = 9 seconds < water mark
  * */
}

/** Create another object to send data through Socket
 * => Run the data sender first, then run the Watermakrs*/
import java.net.ServerSocket

object DataSender extends App {
  val serverSocket = new ServerSocket(12345)
  val socket = serverSocket.accept() // blocking call => spark will connect to 12345
  val printer = new PrintStream(socket.getOutputStream)  // java class to send data through socket

  // printer.println("hello") // hello will get through socket and spark won't read that

  println("socket accepted")

  def example1() = {
    Thread.sleep(7000)
    printer.println("7000,blue")
    Thread.sleep(1000)
    printer.println("8000,green")
    Thread.sleep(4000)
    printer.println("14000,blue")
    Thread.sleep(1000)
    printer.println("9000,red") // discarded: older than the watermark
    Thread.sleep(3000)
    printer.println("15000,red") // discarded: older than the watermark
    printer.println("8000,blue")
    Thread.sleep(1000)
    printer.println("13000,green")
    Thread.sleep(500)
    printer.println("21000,green")
    Thread.sleep(3000)
    printer.println("4000,purple") // expect to be dropped => it's dropped
    Thread.sleep(2000)
    printer.println("17000,green") // expect to be dropped
  }

  example1()
}
