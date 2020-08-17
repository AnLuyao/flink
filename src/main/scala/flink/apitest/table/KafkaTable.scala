package flink.apitest.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
  * Created by anluyao on 2020-08-17 18:21
  */
object KafkaTable {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment,settings)
    //从kafka读取数据
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
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    //做转换操作
    //将DataStream转换成Table
    val sensorTable: Table = tableEnvironment.from("kafkaInputTable")
    //对Table进行转换操作得到结果表
    val resultTable: Table = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")
    val aggResultTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count() as 'count)
    //定义一个连接到kafka的输出表
    tableEnvironment.connect(new Kafka()
      .version("0.11")
      .topic("sensor_out")
      .property("bootstrap.servers", "localhost:9092")
      .property("zookeeper.connect", "localhost:2181")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")
    //将结果表输出
    resultTable.insertInto("kafkaOutputTable")

    tableEnvironment.execute("kafka table test")



  }

}
