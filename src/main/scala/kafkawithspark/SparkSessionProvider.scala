package kafkawithspark

import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object SparkSessionProvider {
  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("Kafka Spark Consumer")
    //.config("spark.driver.bindAddress", "127.0.0.1")
    .master("local[2]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "5")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.executor.instances", "2")
    //.config("spark.master", "http://rspllt428.rishabh.com:7077")
    .getOrCreate()

  import org.apache.log4j.Logger

  // Initialize logger
  implicit val logger: Logger = Logger.getLogger("org"/*getClass.getName*/)
  implicitly(logger.setLevel(Level.INFO))

}
