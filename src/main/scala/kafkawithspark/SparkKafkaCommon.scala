package kafkawithspark

import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig}
import java.util.Properties
import kafkawithspark.SparkSessionProvider._
import org.apache.spark.sql

object SparkKafkaCommon {

  val props = new Properties()
  props.load(getClass.getResourceAsStream("/config.properties"))

  val topics = props.getProperty("topic")
  val topicList = topics.split(",").map(_.trim)
  val topic = topicList(1)

  val schemaRegistryUrl = props.getProperty("schema.registry.url")
  val outputPath = props.getProperty("outputPath").trim
  val checkpointLocation = props.getProperty("checkpointLocation").trim

  /**
   * AbrisConfig.fromConfluentAvro: This sets up the Abris configuration to use the Confluent Avro deserialization format.
   * downloadReaderSchemaByLatestVersion: This instructs Abris to download the latest version of the Avro schema from the Schema Registry.
   * andTopicNameStrategy(topic): This specifies the topic name strategy for Abris, which in this case is set to the value of the topic variable. This strategy tells Abris how to determine the Avro schema based on the Kafka topic name.
   * usingSchemaRegistry("http://localhost:8081"): This provides the URL of the Schema Registry where Abris can fetch the Avro schema.
   */

  def abrisConfig1(topic: String, schemaRegistryUrl: String): FromAvroConfig = {
    AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(topic)
      .usingSchemaRegistry(schemaRegistryUrl)
  }

  def readFromTopic(startingOffsets: String, batchSize: String): sql.DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094")
      .option("startingOffsets", startingOffsets)
      .option("subscribe", topic)
      .option("maxOffsetsPerTrigger", batchSize)
      .option("failOnDataLoss", false)
      .load()
  }

}
