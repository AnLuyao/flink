package flink.apitest.table.Function

import flink.apitest.stream.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}
import org.apache.flink.types.Row

/**
  * Created by AnLuyao on 2020-08-23 18:02
  */
object TableFunction {
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
    val split = new Split("_")
    // 调用Table API,TableFunction使用的时候需要调用joinLateral方法
    val resultTable: Table = sensorTable
      .joinLateral(split('id) as('word, 'length))
      .select('id, 'ts, 'word, 'length)
    // SQL实现
    tableEnvironment.createTemporaryView("sensor", sensorTable)
    tableEnvironment.registerFunction("split", split)
    val resultSqlTable: Table = tableEnvironment.sqlQuery(
      """
        |select id,ts,word,length
        |from
        |sensor,lateral table(split(id)) as splitid(word,length)
      """.stripMargin)

    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("sql result")

    environment.execute("table function test")
  }

}

//自定义一个TableFunction,对一个string，输出用某个分隔符切分之后的（word,word.length）
class Split(seperstor: String) extends TableFunction[(String, Int)] {
  def eval(str: String) = {
    str.split(seperstor).foreach(word => {
      collect((word, word.length))
    })
  }
}

/*
//自定义聚合函数
class MyAgg() extends AggregateFunction[Int, Double] {
  override def getValue(acc: Double): Int = ???

  override def createAccumulator(): Double = ???

  def accumulate(acc: Double, field: String) = {

  }
}*/
