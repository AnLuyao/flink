package flink.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

/**
  * Created by anluyao on 2020-08-10 17:56
  */
object ProcessFunction {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val dataStream: DataStream[SensorReading] = environment
      .addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
      .map(x => {
        val dataArray: Array[String] = x.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
    //检测每一个传感器温度是否连续上升，在10s内连续上升就报警
    //    val warningStream: KeyedStream[SensorReading, Tuple] =
    val warningStream: DataStream[String] = dataStream.keyBy("id")
      .process(new TempIncreaseWarning(10000L))
    warningStream.print()

    environment.execute("process function job")

  }
}

//自定义KeyedProcessFunction
class TempIncreaseWarning(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String] {
  //  由于需要和之前的温度值作对比，所以将上一个温度保存成状态
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  //为了方便删除定时器，还需要保存定时器的时间戳
  lazy val curTimeTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-timestamp", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //    首先取出状态
    val lastTemp: Double = lastTempState.value()
    val curTimerTs: Long = curTimeTsState.value()

    //将上次温度值的状态更新成当前数据的温度值
    lastTempState.update(value.temperature)

    //判断当前温度值，如果比之前温度高，并且没有定时器的话就注册10s后的定时器
    if (value.temperature > lastTemp && curTimerTs == 0) {
      val ts: Long = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      curTimeTsState.update(ts)
    }
    //如果温度下降，那就删除定时器
    else if (value.temperature < lastTemp) {
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      //清空状态
      curTimeTsState.clear()

    }

  }

  //定时器触发，说明10s内没有来下降的温度值就进行报警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

    out.collect("温度值连续" + interval / 1000 + "秒上升")
    curTimeTsState.clear()
  }
}
