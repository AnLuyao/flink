package flink.apitest.table

import flink.apitest.stream.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
  * Created by anluyao on 2020-08-17 17:00
  * 输出表计算的结果到文件
  */
object OutPutTable {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val inputStream: DataStream[String] = environment.readTextFile("/Users/anluyao/workspace/bigdata-flink/src/main/resources/sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(x => {
      val dataArray: Array[String] = x.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    //创建表环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment,settings)

    //将DataStream转换成Table
    val sensorTable: Table = tableEnvironment.fromDataStream(dataStream,'id,'temperature as 'temp,'timestamp as 'ts)
    //对Table进行转换操作得到结果表
    val resultTable: Table = sensorTable.select('id, 'temp)
      .filter('id === "sensor_1")
    val aggResultTable: Table = sensorTable.groupBy('id)
      .select('id, 'id.count() as 'count)


    //定义一张输出表，这就是要写入数据的TableSink
    tableEnvironment
      .connect(new FileSystem().path("/Users/anluyao/workspace/bigdata-flink/src/main/resources/out.txt"))
        .withFormat(new Csv())
        .withSchema(new Schema()
            .field("id",DataTypes.STRING())
            .field("temp",DataTypes.DOUBLE())
        )
        .createTemporaryTable("outputTable")
    //将结果表写入TableSink
    resultTable.insertInto("outputTable")

//    sensorTable.printSchema()
//    sensorTable.toAppendStream[(String,Double,Long)].print()
    println(tableEnvironment.explain(resultTable))
    tableEnvironment.execute("output table")

  }

}
