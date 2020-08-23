package flink.apitest.table

import java.sql.Timestamp

import flink.apitest.stream.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
  * Created by anluyao on 2020-08-17 19:30
  */
object TimeAndWindow {
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
    val sensorTable: Table = tableEnvironment.fromDataStream(dataStream,
      'id, 'timestamp.rowtime() as 'ts, 'temperature)

    //窗口操作
    //1.1Group 窗口，开一个10s滚动窗口，统计每个传感器温度的数量
    val groupResultTable: Table = sensorTable
      .window(Tumble over 10.seconds on 'ts as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count(), 'tw.end())

    //1.2. Group 窗口SQL实现
    tableEnvironment.createTemporaryView("sensor", sensorTable)
    val groupResultSqlTable: Table = tableEnvironment.sqlQuery(
      """
        |select id,count(id),tumble_end(ts,interval '10' second)
        |from sensor
        |group by id,tumble(ts,interval '10' second)
      """.stripMargin)

    //2.1 Over窗口，对每个传感器统计每一行数据与前两行数据的平均温度
    val overResultTable: Table = sensorTable
      .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'w)
      .select('id, 'ts, 'id.count over 'w, 'temperature.avg over ('w))

    // 2.2 Over窗口的SQL实现
    val overResultSqlTable: Table = tableEnvironment.sqlQuery(
      """
        |select
        | id,
        | ts,
        | count(id) over w,
        | avg(temperature) over w
        |from sensor
        |window w as(
        | partition by id
        | order by ts
        | rows between 2 preceding and current row
        |)
      """.stripMargin)



    //转换成流打印输出
    overResultTable.toAppendStream[Row].print("group result")
    overResultSqlTable.toAppendStream[Row].print("overResultSqlTable")


    environment.execute(" time and window test")

  }

}
