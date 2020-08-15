package flink.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * Created by anluyao on 2020-02-28 19:11
  */
object MyKafkaUtil {
  private val properties = new Properties()
  properties.setProperty("bootstrap.servers","localhost:9092")
  properties.setProperty("group.id","gmall")

  def getKafkaSource(topic:String): FlinkKafkaConsumer011[String] ={
    val kafkaConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),properties)
    kafkaConsumer
  }

}
