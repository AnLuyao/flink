package flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * Created by anluyao on 2020-02-26 20:02
  */
object DataStreamWcApp {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val hostname: String = tool.get("host")
    val port: Int = tool.getInt("port")
    val dataStream: DataStream[String] = environment.socketTextStream(hostname, port)
    val sumDstream: DataStream[(String, Int)] = dataStream
      .flatMap(_.split(" ")).slotSharingGroup("1")
      .filter(_.nonEmpty)
      .map((_, 1)).disableChaining()
      .keyBy(0)
      .sum(1).startNewChain()
    sumDstream.print().setParallelism(1)
    environment.execute()
  }


}
