package integrations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, struct, to_json, upper}
import common.carsSchema

object IntegratingKafka extends App {
  // Iniititial spark session
  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def readFromKafka() = {
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")  // ("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "rockthejvm")                    // topic to read data, can have multiple topics, split by comma ("rockthejvm, topicb")
      .load()

    kafkaDF
      .select(col("topic"),
        expr("cast(value as string) as stringValue"),
        col("timestamp"),
        col("timestampType"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // Write from spark to Kafka
  def writeToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .format("json")
      .load("src/main/resources/data/cars")

    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "Name as value")

    /**
     * Without checkpoints the writing to Kafka will fail, checkpoints folder will be created to mark up which data has been ingested */
    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints")  // without checkpoints the writing to Kafka will fail, checkpoints folder will be created
      .start()
      .awaitTermination()
  }

  /**
   * Exercise: Write the whole cars data structures to Kafka as Json
   * Use struct columns and to_json, struc function in sparks.
   * */

  def writeCarsToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/tests")

    val carsJsonKafka = carsDF.select(
      upper(col("Name")).as("key"),
      to_json(struct(col("Name"), col("Horsepower"), col("Origin"))).cast("String").as("value")
    )

    /**
     * Without checkpoints the writing to Kafka will fail, checkpoints folder will be created to mark up which data has been ingested*/
    carsJsonKafka.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

  writeCarsToKafka()



}
