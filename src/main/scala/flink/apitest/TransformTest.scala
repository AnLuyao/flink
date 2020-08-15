package flink.apitest

import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * Created by anluya o on 2020-08-01 17:30
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val inputStreamFromFile: DataStream[String] = environment.readTextFile("/Users/anluyao/workspace/bigdata-flink/src/main/resources/sensor.txt")
    //1.基本转换操作
    val dataStream: DataStream[SensorReading] = inputStreamFromFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    //2.分组滚动聚合操作
    val aggStream: DataStream[SensorReading] = dataStream.keyBy("id")

      //      .keyBy(data=>data.id)
      //      .keyBy(new MyIdSelector)
      //      .minBy("temperature")
      //聚合出每个sensor的最大时间戳和最小温度值
      //      .reduce((curRes, newData) =>
      //      SensorReading(curRes.id,
      //        curRes.timestamp.max(newData.timestamp),
      //        curRes.temperature.min(newData.temperature)
      //      ))
      .reduce(new MyReduce)

    //3.分流
    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 30) {
        Seq("high")
      } else Seq("low")
    })
    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")

    //4.合流
    val warningStream: DataStream[(String, Double)] = highTempStream.map(data => (data.id, data.temperature))
    val connectStreams: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowTempStream)
    val resultStream: DataStream[Product] = connectStreams.map(
      warningData => (warningData._1, warningData._2, "high temp warning"),
      lowTempData => (lowTempData.id, "normal")
    )

    val unionStream: DataStream[SensorReading] = highTempStream.union(lowTempStream).union(allTempStream)

    //    dataStream.print()
    resultStream.print("resultStream")

    environment.execute("transform test job")
  }

}

class MyIdSelector() extends KeySelector[SensorReading, String] {
  override def getKey(in: SensorReading): String = in.id
}

//自定义函数类ReduceFunction
class MyReduce() extends ReduceFunction[SensorReading] {
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id, t.timestamp.max(t1.timestamp), t.temperature.min(t1.temperature))
  }
}

//自定义函数类MapFunction
class MyMapper extends MapFunction[SensorReading, (String, Double)] {
  override def map(t: SensorReading): (String, Double) = (t.id, t.temperature)
}

class MyRichMapper extends RichMapFunction[SensorReading, Int] {
  override def open(parameters: Configuration): Unit = {}

  override def map(in: SensorReading): Int = in.timestamp.toInt

  override def close(): Unit = {}
}