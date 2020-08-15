package util.kafkaUtil

import java.util.Properties

import common.RandomNum
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Created by anluyao on 2020-02-14 22:20
  */
object KafkaProducer {
  def BROKER_LIST = "localhost:9092"

  def TOPIC = "sensor"

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put("metadata.broker.list", BROKER_LIST)
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](properties)
    while (true) {
      for (i <- 0 to 10) {
        producer.send(new ProducerRecord(TOPIC, "key-" + i, "sensor_" + i + "," + System.currentTimeMillis() + "," + i+30))
      }
      Thread.sleep(3000)
    }
    producer.close()
  }

}
