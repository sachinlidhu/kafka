package kafkawithspark

import kafkawithspark.SparkKafkaCommon._
import org.apache.spark.sql.functions._
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.FromAvroConfig
import kafkawithspark.SparkSessionProvider._
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame

object KafkaSparkConsumerCommon {
  val abrisConfig: FromAvroConfig = abrisConfig1(topic, schemaRegistryUrl)

  def rawReadFromTopic: DataFrame = {
    val customOffsets = "{\"generic-orders1\":{\"0\":5170,\"1\":5670,\"2\":5515}}"
    val rawReadFromTopic = readFromTopic(customOffsets, "1000")
    logger.info("----read from kafka without any deserialization-----")
    rawReadFromTopic.printSchema()
    rawReadFromTopic
  }

  def deserializedAvro: sql.DataFrame = {
    val deserializedAvro =  rawReadFromTopic.select(from_avro(col("value"), abrisConfig).as("data"))
      .select(col("data.*"))
    logSchemaInfo(deserializedAvro)
    deserializedAvro
  }

  private def logSchemaInfo(dataframe: DataFrame): Unit ={
    logger.info("deserialized DF Avro schema: ")
    dataframe.printSchema()
  }

  // Add UTC timestamp column
  val withTimestamp = deserializedAvro.withColumn("timestamp", current_timestamp())

  // Extract year, month, day, and hour from the timestamp
  val partitionedData = withTimestamp
    .withColumn("year", year(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("day", dayofmonth(col("timestamp")))
    .withColumn("hour", hour(col("timestamp")))


  // Write partitioned data to the filesystem
  val timePartitioner = partitionedData.repartition(2)
    .writeStream
    .outputMode("append")
    .format("avro")
    .partitionBy("year", "month", "day", "hour")
    .option("path", outputPath)
    .option("checkpointLocation", checkpointLocation)
    .start()


  implicit val a = timePartitioner.awaitTermination()

}
