package flink.apitest.table

import flink.apitest.stream.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._

/**
  * Created by anluyao on 2020-08-16 19:56
  */
object TableExample {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    //读取数据 创建DataStream
    val inputStream: DataStream[String] = environment.readTextFile("/Users/anluyao/workspace/bigdata-flink/src/main/resources/sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(x => {
      val dataArray: Array[String] = x.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    //创建表执行环境
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment)

    //基于dataStream数据流 转换成一张表，然后进行操作
    val dataTable: Table = tableEnvironment.fromDataStream(dataStream)

    //调用Table api,得到转换结果
    val resultTable: Table = dataTable
      .select("id,temperature")
      .filter("id == 'sensor_1'")
    //或者直接写SQL得到转换结果
    val resultSqlTable: Table = tableEnvironment.sqlQuery("select id,temperature from " + dataTable + " where id = 'sensor_1'")

    //转换回数据流打印输出
    val resultStream: DataStream[(String, Double)] = resultSqlTable.toAppendStream[(String, Double)]

    resultStream.print("result")
//    resultTable.printSchema()

    environment.execute("table example job")

  }

}
