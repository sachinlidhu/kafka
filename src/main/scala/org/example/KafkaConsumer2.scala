/*
package org.example

import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Collections
import java.util.Properties

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import example.avro.User

import scala.jdk.CollectionConverters.IterableHasAsScala

object KafkaConsumer2 extends App {
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

  // Define the time partition format
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HH")
  // Keep track of the sequence number for each time partition
  var sequenceNumberMap: Map[String, Int] = Map.empty

  try {
    while (true) {
      println("Waiting for data poll method...")
      val records = kafkaConsumer.poll(java.time.Duration.ofMillis(10000))

      // Group records by time partition
      val recordsByTimePartition = records.asScala.groupBy { record =>
        val timestamp = record.timestamp() // Assuming timestamps are set in the records
        LocalDateTime.ofEpochSecond(timestamp / 1000, 0, java.time.ZoneOffset.UTC).format(dateTimeFormatter)
      }

      // Iterate over each time partition and write records to files
      recordsByTimePartition.foreach { case (timePartition, records) =>
        println(timePartition)

        // Get the sequence number for the current time partition
        val sequenceNumber = sequenceNumberMap.getOrElse(timePartition, 0) + 2
        sequenceNumberMap += (timePartition -> sequenceNumber)
        val outputFile = new File(s"C:\\Users\\sachin.lidhu\\Documents\\kafkaData\\output\\$timePartition-$sequenceNumber.txt")
        val writer = new PrintWriter(outputFile)
        try {
          records.foreach { record =>
            writer.println("Key: " + Option(record.key()).getOrElse("Key Not available"))
            writer.println("Message received: " + record.value())
          }
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
*/
