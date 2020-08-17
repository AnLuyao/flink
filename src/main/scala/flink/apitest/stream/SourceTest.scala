package flink.apitest.stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
  * Created by anluyao on 2020-08-01 14:54
  */
//输入数据的样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    //    创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    //1.从集合中读取数据
    val stream1: DataStream[SensorReading] = environment.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_1", 1547718122, 35.0),
      SensorReading("sensor_1", 1547718198, 39.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))
    //2.从文件中读取
    val stream2: DataStream[String] = environment.readTextFile("/Users/anluyao/workspace/bigdata-flink/src/main/resources/sensor.txt")

    //3.socket文本流
    val socketStream: DataStream[String] = environment.socketTextStream("localhost", 7777)

    //4.从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val kafkaStream: DataStream[String] = environment.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    //5.自定义source
    val mySensorStream: DataStream[SensorReading] = environment.addSource(new MySensorSource())
    //打印输出
    mySensorStream.print("mySensorStream")
    environment.execute("source test job")


  }
}

/**
  * 实现一个自定义的SourceFunction，自动生成测试数据
  */
class MySensorSource() extends SourceFunction[SensorReading] {
  //定义一个flag表示数据源是否正常运行
  var running: Boolean = true

  /**
    * 随机生成SensorReading数据
    *
    * @param sourceContext
    */
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //    定义一个随机数发生器
    val random = new Random()
    //    随机生成10个传感器的温度值，并且不停在之前温度基础上更新（随机上下波动）
    //    首先生成10个传感器的初始温度
    var curTemps = 1.to(10).map(i => ("sensor_" + i, 60 + random.nextGaussian() * 20))
    //无限循环 生成随机数据流
    while (running) {
      //在当前温度基础上，随机生成微小波动
      curTemps = curTemps.map(data => (data._1, data._2 + random.nextGaussian()))
      //获取当前系统时间
      val curTs: Long = System.currentTimeMillis()
      //包装成样例类，用sourceContext发出数据
      curTemps.foreach(x => {
        sourceContext.collect(SensorReading(x._1, curTs, x._2))
      })
      //定义间隔时间
      Thread.sleep(1000L)

    }

  }


  override def cancel(): Unit = running = false
}

