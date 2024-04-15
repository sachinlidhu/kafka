/*
package org.example

import java.util.Collections
import java.util.Properties
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import example.avro.User
import scala.jdk.CollectionConverters.IterableHasAsScala
object KafkaConsumer extends App {
  // Kafka consumer properties
  val properties = new Properties()
  // normal consumer
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.put("group.id", "consumer-group-v1")
  properties.put("auto.commit.enable", "false")
  //properties.put("auto.offset.reset", "earliest")
  // avro part (deserializer)
  properties.setProperty("key.deserializer", classOf[StringDeserializer].getName)
  properties.setProperty("value.deserializer", classOf[KafkaAvroDeserializer].getName)
  properties.setProperty("schema.registry.url", "http://localhost:8081")
  properties.setProperty("specific.avro.reader", "true")
  val kafkaConsumer = new KafkaConsumer[String, User](properties)
  val topic = "user-topic"
  kafkaConsumer.subscribe(Collections.singleton(topic))
  println("Kafka Consumer Application Started ...")
  println("Waiting for data...")
  try {
    while (true) {
      val records = kafkaConsumer.poll(java.time.Duration.ofMillis(100))
      records.asScala.foreach { record =>
        println("Key: " + Option(record.key()).getOrElse("Ket Not available"))
        println("Message received: " + record.value())
      }
    }
  } catch {
    case ex: Exception =>
      println("Failed to read Kafka message.")
      println(ex.getMessage)
  } finally {
    kafkaConsumer.close()
  }
}
*/
