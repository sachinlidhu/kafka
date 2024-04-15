package org.example

import java.io.File
import java.util.Properties
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import io.confluent.kafka.serializers.{ AbstractKafkaSchemaSerDeConfig, KafkaAvroSerializer}
import com.github.javafaker.Faker
import example.avro.User
import org.apache.kafka.common.serialization.StringSerializer
object KafkaProducer extends App {
  //  def main (args: Array[String]): Unit = {
  // Load the Avro schema
  //val userSchema = new Schema.Parser().parse(new File("src/main/resources/avro/user.avsc"))
  // Configure producer properties
  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", "localhost:9092")
  producerProps.put("key.serializer", classOf[KafkaAvroSerializer].getName)
  producerProps.put("value.serializer", classOf[KafkaAvroSerializer].getName)
  producerProps.put("schema.registry.url", "http://localhost:8081")
  //    producerProps.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false)
  // Create a Kafka producer
  val producer = new KafkaProducer[String, User](producerProps)
  // Create a Faker instance
  val faker = new Faker()
  // Create a user record
  //    val user = new GenericData.Record(userSchema)
  val user = User.newBuilder().
    setId( faker.idNumber().valid())
    .setName( faker.name().fullName())
    .setEmail(faker.internet().emailAddress())
    .setAge(faker.number().numberBetween(18, 65))
    .setMobNo(faker.number().randomDigitNotZero())
    //.setRegion( faker.name().fullName())
    .setPinCode(faker.number().randomDigitNotZero())
    //.setCity(faker.name().firstName())
    //.setState(faker.name().firstName())
    .build()
  // Log the user record before sending
  //println(s"Producing Avro record: id=${user.get("id")}, name=${user.get("name")}, email=${user.get("email")}, age=${user.get("age")}, mob_no=${user.get("mob_no")}")
  val producerRecord = new ProducerRecord[String,User]("user-topic", user)
  // Send the user record to the Kafka topic
  producer.send(producerRecord)
  // Close the producer
  producer.close()
  // Log that the message has been sentAA
  println("Avro record sent to Kafka topic")
}
