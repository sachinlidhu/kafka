package kafkawithspark

import KafkaSparkConsumerCommon._

object KafkaSparkConsumer extends App {

  rawReadFromTopic
  deserializedAvro
  timePartitioner
}
