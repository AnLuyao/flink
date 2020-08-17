package flink.apitest.stream.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import flink.apitest.stream.SensorReading
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RichFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
  * Created by anluyao on 2020-08-04 22:21
  */
object MysqlSink {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val inputStream: DataStream[String] = environment.readTextFile("/Users/anluyao/workspace/bigdata-flink/src/main/resources/sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(x => {
      val dataArray: Array[String] = x.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    dataStream.addSink(new MyJdbcSink())


    environment.execute("mysql sink test")

  }
}

//自定义一个SinkFunction
class MyJdbcSink extends RichSinkFunction[SensorReading] {
  //定义SQL连接，以及预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  //在open生命周期方法中创建连接以及预编译语句
  override def open(configuration: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("insert into temp(sensor,temperature) values(?,?)")
    updateStmt = conn.prepareStatement("update temp temperature = ? where sensor = ?")
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]) = {
    //执行更新语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    //如果刚才没有更新数据，那么执行插入操作
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }

  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
