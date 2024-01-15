package integrations

import org.apache.spark.sql.{ SparkSession, Dataset }
import common._

object IntegratingJDBC extends App {

  val spark = SparkSession.builder()
    .appName("Intergrating JDBC")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def writeStreamToPostgres(host: String, port: Any, database: String, user: String, password: String) = {

    val url: (String, String) => String = (host, database) => s"jdbc:postgresql://$host:$port/$database"
//    val url2 = (host: String, database: String) => s"jdbc:postgresql://$host:$port/$database"

    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Cars]

    // can not write directly to jdbc
    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Cars], _: Long) =>
        // each executor can control the batch
        // batch is a STATIC Dataset / DataFrame

        batch.write
          .format("jdbc")
          .mode("append")
          .option("driver", "org.postgresql.Driver")
          .option("url", url(host, database))
          .option("user", user)
          .option("password", password)
          .option("dbtable", "public.cars")
          .save()
      }
      .start()
      .awaitTermination()
  }

  writeStreamToPostgres(
    host="localhost",
    port=5432,
    database = "rtjvm",
    user = "docker",
    password = "docker"
  )

}
