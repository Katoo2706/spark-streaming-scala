package integrations

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

// https://sparkbyexamples.com/kafka/apache-kafka-consumer-producer-in-scala/
object KafkaProducerApp extends App {

  def produceMessage(topic: String, key: Option[String] = None, message: Any) = {
    val props: Properties = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all") // https://viblo.asia/p/apache-kafka-producer-gui-message-den-kafka-bang-kafka-python-maGK7B7a5j2

    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](
      topic,
      key.getOrElse("undefined"), // Using getOrElse to handle the optional key
      message.toString)

    producer.send(record)

    producer.close()
  }

  produceMessage(topic = "rockthejvm",
    key = Some("random topic"),
    message = List(34, 5, 4))

  produceMessage(topic = "rockthejvm", message = "This message has no key")

}
