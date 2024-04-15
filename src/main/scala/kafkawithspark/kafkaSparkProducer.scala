package kafkawithspark

import kafkawithspark.SparkSessionProvider._
import kafkawithspark.KafkaSparkProducerCommon._

object kafkaSparkProducer extends App{

  producer
  produce_the_data
  closeProducer

}
