package flink.apitest.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

/**
  * Created by anluyao on 2020-08-17 15:15
  */
object TableApi {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    //    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment)
    //1.创建表环境
    //1.1 创建老版本流式查询环境
    val envSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, envSettings)

    //1.2 创建老版本的批式查询环境
    val batchEnvironment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnvironment: BatchTableEnvironment = BatchTableEnvironment.create(batchEnvironment)

    //1.3 创建blink版本的流式查询环境
    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bstableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, bsSettings)

    //1.4创建blink版本的批式查询环境
    val bbSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val bbtableEnvironment: TableEnvironment = TableEnvironment.create(bbSettings)

    // 2.从外部系统读取数据，在环境中注册表
    // 2.1连接到文件系统（CSV）
    val filePath = "/Users/anluyao/workspace/bigdata-flink/src/main/resources/sensor.txt"
    tableEnvironment.connect(new FileSystem().path(filePath))
      .withFormat(new Csv()) //定义读取数据之后的格式化方法
      .withSchema(new Schema() //定义表结构
      .field("id", DataTypes.STRING())
      .field("timestamp", DataTypes.BIGINT())
      .field("temperatore", DataTypes.DOUBLE())
    )
      .createTemporaryTable("inputTable") //注册一张表

    // 2.2连接到kafka
    tableEnvironment.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("bootstrap.servers", "localhost:9092")
      .property("zookeeper.connect", "localhost:2181")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperatore", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")


    //转换成流打印输出
    val sensorTable: Table = tableEnvironment.from("kafkaInputTable")
    sensorTable.toAppendStream[(String, Long, Double)].print()
    environment.execute("table api test job")

  }

}
