package flink

import flink.util.MyKafkaUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._

/**
  * Created by anluyao on 2020-02-28 19:06
  */
object StreamApiApp {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val kafkaSource: FlinkKafkaConsumer011[String] = MyKafkaUtil.getKafkaSource("topic")
    val dstream: DataStream[String] = environment.addSource(kafkaSource)
    dstream.print()
    environment.execute()
  }
}
