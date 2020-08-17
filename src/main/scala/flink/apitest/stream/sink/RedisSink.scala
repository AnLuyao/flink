package flink.apitest.stream.sink

import flink.apitest.stream.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * Created by anluyao on 2020-08-03 23:30
  */
object RedisSink {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val inputStream: DataStream[String] = environment.readTextFile("/Users/anluyao/workspace/bigdata-flink/src/main/resources/sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(x => {
      val dataArray: Array[String] = x.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    //定义一个Redis的配置类
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()
    //定义一个RedisMapper
    val myMapper: RedisMapper[SensorReading] = new RedisMapper[SensorReading] {
      /**
        * 定义保存到Redis的命令
        *
        * @return
        */
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
      }

      override def getKeyFromData(data: SensorReading): String = data.id

      override def getValueFromData(data: SensorReading): String = data.temperature.toString
    }

    dataStream.addSink(new RedisSink[SensorReading](conf, myMapper))

    environment.execute("redis sink test")

  }

}
