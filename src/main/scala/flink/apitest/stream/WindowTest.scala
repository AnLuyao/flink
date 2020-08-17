package flink.apitest.stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

/**
  * Created by anluyao on 2020-08-05 21:45
  */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.getConfig.setAutoWatermarkInterval(100L)
    val inputStreamFromFile: DataStream[String] = environment.readTextFile("/Users/anluyao/workspace/bigdata-flink/src/main/resources/sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStreamFromFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val kafkaStream: DataStream[SensorReading] = environment
      .addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
      .map(x => {
        val dataArray: Array[String] = x.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      //      .assignAscendingTimestamps(_.timestamp)
      //      .assignTimestampsAndWatermarks(new MyPeriodicWMAssigner(1000l))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp
    })


    val resultStream: DataStream[(Long, Int)] = kafkaStream
      .keyBy("id")
      //      .countWindow(10, 2)
      //      .window(TumblingProcessingTimeWindows.of(Time.hours(1), Time.hours(-8)))
      //      .timeWindow(Time.hours(1),Time.minutes(1))
      //      .window(EventTimeSessionWindows.withGap(Time.minutes(1))) //回话窗口
      .timeWindow(Time.seconds(15), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[SensorReading]("late"))
      //      .reduce(new MyReduce)
      .apply(new MyWindowFunction)
    dataStream.print("data")
    resultStream.getSideOutput(new OutputTag[SensorReading]("late"))
    resultStream.print("result")
    environment.execute("window api test")

  }
}

//自定义一个全窗口函数
class MyWindowFunction() extends WindowFunction[SensorReading, (Long, Int), Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[(Long, Int)]): Unit = {
    out.collect(window.getStart, (input.size))
  }
}


//自定义一个周期性生成watermark的assigner
class MyPeriodicWMAssigner(lateness: Long) extends AssignerWithPeriodicWatermarks[SensorReading] {
  //需要两个关键参数：延迟时间和当前所有数据中最大的时间戳
  //  val lateness: Long = 1000l
  var maxTs: Long = Long.MinValue + lateness

  override def getCurrentWatermark: Watermark =
    new Watermark(maxTs - lateness)

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp)
    element.timestamp
  }
}

//自定义一个断点式生成watermark的Assigner
class MyPunctuatedWMAssigner extends AssignerWithPunctuatedWatermarks[SensorReading] {
  val lateness: Long = 1000l

  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if (lastElement.id == "sensor_1") {
      new Watermark(extractedTimestamp - lateness)
    } else null
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp
  }
}
