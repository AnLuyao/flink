package flink.apitest.stream

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Created by anluyao on 2020-08-11 23:39
  */
object SideOutPut {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val inputStream: DataStream[String] = environment.socketTextStream("localhost", 7777)
    val dataStream: DataStream[SensorReading] = inputStream.map(x => {
      val dataArray: Array[String] = x.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })
    //用ProcessFunction的侧输出流实现分流操作
    val highTempStream: DataStream[SensorReading] = dataStream
      .process(new SplitTempProcessor(30.0))
    val lowTempStream: DataStream[(String, Double, Long)] = highTempStream
      .getSideOutput(new OutputTag[(String, Double, Long)]("low-temp"))

    highTempStream.print("high")
    lowTempStream.print("low")
    environment.execute("side output job")
  }
}

//自定义ProcessFunction,用户区分高低温度的数据
class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    //判断当前数据的温度值，如果大于阈值，输出到主流，否则输出到侧输出流
    if (value.temperature > threshold) {
      out.collect(value)
    } else
      ctx.output(new OutputTag[(String, Double, Long)]("low-temp"),
        (value.id, value.temperature, value.timestamp))
  }
}