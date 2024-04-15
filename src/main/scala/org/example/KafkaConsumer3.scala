package org.example

import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Collections, Properties}

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import example.avro.User

import scala.collection.JavaConverters._

object KafkaConsumer3 extends App {
  // Kafka consumer properties
  val properties = new Properties()
  // normal consumer
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.put("group.id", "consumer-group-v1")
  properties.put("auto.commit.enable", "false")
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
      val records = kafkaConsumer.poll(java.time.Duration.ofMillis(10000))

      records.asScala.foreach { record =>
        // Get the current timestamp
        val timestamp = LocalDateTime.ofEpochSecond(record.timestamp() / 1000, 0, java.time.ZoneOffset.UTC)

        // Create the output directory
        val yearMonthDir = s"C:\\Users\\sachin.lidhu\\Documents\\kafkaData\\output\\${timestamp.getYear}-${timestamp.getMonthValue}"
        val yearMonthDirFile = new File(yearMonthDir)
        if (!yearMonthDirFile.exists()) {
          yearMonthDirFile.mkdirs()
        }

        val outputFile = new File(yearMonthDir, s"${timestamp.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))}.txt")

        // Write the record to the output file
        val writer = new PrintWriter(outputFile)
        try {
          writer.println("Key: " + Option(record.key()).getOrElse("Key Not available"))
          writer.println("Message received: " + record.value())
        } finally {
          writer.close()
        }
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
