package util.kafkaUtil

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

/**
  * Created by anluyao on 2020-02-29 16:01
  */
object KafkaConsumer {
  def BROKER_LIST = "localhost:9092"

  def TOPIC = "kafka_test"
  def main(args: Array[String]): Unit = {
    val className: String = getClass.getName.split("\\.").last
    val properties: Properties = new Properties()
    properties.put("bootstrap.servers",BROKER_LIST)
    properties.put("group.id","test"+className)
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String,String](properties)
//    consumer.subscribe(util.Collections)

  }

}
