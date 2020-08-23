package flink.apitest.table.Function

import flink.apitest.stream.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
  * Created by AnLuyao on 2020-08-23 15:43
  */
object ScalarFunction {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream: DataStream[String] = environment.readTextFile("/Users/anluyao/workspace/bigdata-flink/src/main/resources/sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(x => {
      val dataArray: Array[String] = x.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //将DataStream转换成Table
    val sensorTable: Table = tableEnvironment.fromDataStream(dataStream, 'id, 'timestamp.rowtime() as 'ts, 'temperature)

    // 创建一个udf实例
    val hashCode = new HashCode(10)
    // 调用Table API
    val resultTable: Table = sensorTable.select('id, 'ts, hashCode('id))
    // SQL实现
    tableEnvironment.createTemporaryView("sensor", sensorTable)
    tableEnvironment.registerFunction("hashcode", hashCode)
    val resultSqlTable: Table = tableEnvironment.sqlQuery("select id,ts,hashcode(id) from sensor")

    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("sql result")

    environment.execute("scalar function test")
  }
}

//自定义标量函数
class HashCode(factor: Int) extends ScalarFunction {
  //必须要实现一个eval方法，其参数是当前传入字段，输出是Int类型的hash值
  def eval(str: String): Int = {
    str.hashCode * factor
  }


}