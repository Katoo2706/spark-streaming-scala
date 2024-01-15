package structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

/**
 * - Join data in micro-batches
 * - Structure Streaming join API
 *
 * Restricted joins:
 * -> Stream joining with static: RIGHT outer join/FULL outer join/ RIGHT semi not permitted
 * -> Static joining with stream: LEFT outer join/FULL outer join/ LEFT semi not permitted
 *
 * Since spark 2.3, we have stream vs stream joins*/

// Spark 3.0 provides an option recursiveFileLookup to load files from recursive sub-folders.
object StreamingJoins extends App {
  val spark = SparkSession.builder()
    .appName("Streaming Joins")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  // Spark 3.0 provides an option recursiveFileLookup to load files from recursive sub-folders.


  /** Static joins*/
  val guitarPlayers = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers")

  val guitars = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitars")

  val bands = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/bands")

  guitarPlayers.join(
    right = bands,
    joinExprs = guitarPlayers.col("band") === bands.col("id"),
    joinType = "inner" // default join type
  )


  /** Streaming join with Static*/
  def joinStreamWithStatic() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // a DF with a single column "value" of type String
      .select(from_json(col("value"), bands.schema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")
      // or selectExpr("band.*") // this will show up all column in the schema

    // from_json: Parses a column containing a JSON string into a StructType with the specified schema.
    //            Returns null, in the case of an unparseable string.

    // Execute join statement
    val streamBandsGuitaristsDF = streamedBandsDF.join(
      guitarPlayers,
      guitarPlayers.col("band") === streamedBandsDF.col("id"),
      "inner"
    )

    // Print output to console
    streamBandsGuitaristsDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  /** Stream join with stream*/
  def joinStreamWithStream() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // a DF with a single column "value" of type String
      .select(from_json(col("value"), bands.schema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamedGuitaristsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load() // a DF with a single column "value" of type String
      .select(from_json(col("value"), guitarPlayers.schema).as("guitarPlayer"))
      .selectExpr("guitarPlayer.id as id", "guitarPlayer.name as name", "guitarPlayer.guitars as guitars", "guitarPlayer.band as band")


    // Execute join statement
    val streamedJoin = streamedBandsDF.join(
      streamedGuitaristsDF,
      streamedGuitaristsDF.col("band") === streamedBandsDF.col("id"),
      "inner"
    )
    /*
    * - inner joins are supported
    * - left/right outer joins ARE supported only with "WARTERMARKS"
    * - full outer joins are not supported*/

    streamedJoin.writeStream
      .format("console")
      .outputMode("append") // only append supported for stream vs stream join
      .start()
      .awaitTermination()

  }

  joinStreamWithStream()

}
