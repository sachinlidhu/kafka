package kafkawithspark

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.{GenericData, GenericDatumWriter}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import kafkawithspark.SparkSessionProvider._
import kafkawithspark.DataGeneration.generateOrder
import SparkKafkaCommon._

object KafkaSparkProducerCommon {
  val properties: Properties = {

    val props = new Properties()
    val propsMap = Map(
      "bootstrap.servers" -> "localhost:9092, localhost:9093, localhost:9094",
      "acks" -> "all",
      "retries" -> "10",
      "key.serializer" -> classOf[StringSerializer].getName,
      "value.serializer" -> classOf[KafkaAvroSerializer].getName,
      "schema.registry.url" -> "http://localhost:8081"
    )
    propsMap.foreach { case (key, value) => props.setProperty(key, value) }
    props
  }

  val producer = new KafkaProducer[String, GenericData.Record](properties)

  def produceOrder(producer: KafkaProducer[String, GenericData.Record], topic: String): Int => Unit =
    _ => {
      val order = generateOrder()
      val record = new ProducerRecord[String, GenericData.Record](topic, order)
      producer.send(record)
      logger.info(s"----Produced order event: $order----")
      Thread.sleep(100)
    }

  val produceOrders: Int => Unit = produceOrder(producer, topic)

  val totalRecords = 10000
  val produce_the_data = (1 to totalRecords).foreach(produceOrders)
  val closeProducer = producer.close()
}
