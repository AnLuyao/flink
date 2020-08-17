package flink.apitest.stream.sink

import java.util.Properties

import flink.apitest.stream.SensorReading
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
  * Created by anluyao on 2020-08-03 21:19
  */
object KafkaSink {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    //    val inputStream: DataStream[String] = environment.readTextFile("/Users/anluyao/workspace/bigdata-flink/src/main/resources/sensor.txt")
    val inputStream: DataStream[String] = environment
      .addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    val dataStream: DataStream[String] = inputStream.map(x => {
      val dataArray: Array[String] = x.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble).toString
    })
    inputStream.print()
    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "sinkTest", new SimpleStringSchema()))

    /*    dataStream.addSink(StreamingFileSink.forRowFormat(
          new Path("/Users/anluyao/workspace/bigdata-flink/src/main/resources/out.txt"),
          new SimpleStringEncoder[String]("UTF-8")).build()
        )*/
    environment.execute("kafka sink test")
  }

}
