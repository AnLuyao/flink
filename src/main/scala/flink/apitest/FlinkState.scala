package flink.apitest

import java.util

import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._

/**
  * Created by anluyao on 2020-08-12 21:10
  */
object FlinkState {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    //配置状态后端
    //    environment.setStateBackend(new MemoryStateBackend())
    //    environment.setStateBackend( new FsStateBackend(""))
    //    environment.setStateBackend(new RocksDBStateBackend("",true))
    val inputStream: DataStream[String] = environment.socketTextStream("localhost", 7777)
    val dataStream: DataStream[SensorReading] = inputStream.map(x => {
      val dataArray: Array[String] = x.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    val warningStream: DataStream[(String, Double, Double)] = dataStream
      .keyBy("id")
      .flatMapWithState[(String, Double, Double), Double]({
      case (inputData: SensorReading, None) => (List.empty, Some(inputData.temperature))
      case (inputData: SensorReading, lastTemp: Some[Double]) => {
        val diff: Double = (inputData.temperature - lastTemp.get).abs
        if (diff > 10.0) {
          (List((inputData.id, lastTemp.get, inputData.temperature)), Some(inputData.temperature))
        } else (List.empty, Some(inputData.temperature))
      }
    })
    //      .flatMap(new TempChangeWarningWithFlatMap(10.0))
    warningStream.print()
    environment.execute("state test job")
  }
}

//自定义RichMapFunction
class TempChangeWarningWithMap(threshold: Double) extends RichMapFunction[SensorReading, (String, Double, Double)] {
  //定义状态变量，上一次的温度值
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext
      .getState(new ValueStateDescriptor[Double]("map-last-temp", classOf[Double]))
  }

  override def map(t: SensorReading): (String, Double, Double) = {
    //从状态中取出上次的温度值
    val lastTemp: Double = lastTempState.value()
    //更新状态
    lastTempState.update(t.temperature)
    //跟当前温度值计算差值，然后跟阈值比较，大于就报警
    val diff: Double = (t.temperature - lastTemp).abs
    if (diff > threshold) {
      (t.id, lastTemp, t.temperature)
    } else (t.id, 0.0, 0.0)
  }
}

//自定义RichFlatMapFunction,可输出多个结果
class TempChangeWarningWithFlatMap(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("flatmap-last-temp", classOf[Double]))

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    //从状态中取出上次的温度值
    val lastTemp: Double = lastTempState.value()
    //更新状态
    lastTempState.update(in.temperature)
    //跟当前温度值计算差值，然后跟阈值比较，大于就报警
    val diff: Double = (in.temperature - lastTemp).abs
    if (diff > threshold) {
      collector.collect((in.id, lastTemp, in.temperature))
    }
  }
}

//Keyed state定义实例
class MyProcessor extends KeyedProcessFunction[String, SensorReading, Int] {
  //  lazy val myState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state", classOf[Int]))
  lazy val myListState: ListState[String] = getRuntimeContext.
    getListState(new ListStateDescriptor[String]("my-liststate", classOf[String]))
  lazy val myMapState: MapState[String, Double] = getRuntimeContext.
    getMapState(new MapStateDescriptor[String, Double]("my-mapstate", classOf[String], classOf[Double]))
  lazy val myReducingState: ReducingState[SensorReading] = getRuntimeContext
    .getReducingState(new ReducingStateDescriptor[SensorReading](
      "my-reducingstate",
      new ReduceFunction[SensorReading] {
        override def reduce(t: SensorReading, t1: SensorReading): SensorReading =
          SensorReading(t.id, t.timestamp.max(t1.timestamp), t.temperature.min(t1.temperature))
      },
      classOf[SensorReading]))
  var myState: ValueState[Int] = _

  override def open(parameters: Configuration): Unit = {
    myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state", classOf[Int]))
  }

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, Int]#Context, out: Collector[Int]): Unit = {
    myState.value()
    myState.update(1)
    myListState.add("hello")
    myListState.addAll(new util.ArrayList[String]())
    myMapState.put("sensor_1", 10.0)
    myMapState.get("sensor_1")
  }
}

//operator state demo
class MyMapper() extends RichMapFunction[SensorReading, Long] with ListCheckpointed[Long] {
  var count: Long = 0

  override def map(in: SensorReading): Long = {
    count += 1
    count
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Long] = {
    val stateList = new util.ArrayList[Long]()
    stateList.add(count)
    stateList
  }

  override def restoreState(state: util.List[Long]): Unit = {
    val numList = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    // for 循环
    for (a <- numList) {
      println(a)
    }

    for (countState <- state) {
      count += countState

    }
    /*    val iterator: util.Iterator[Long] = state.iterator()
        while (iterator.hasNext) {
          count += iterator.next()
        }*/
  }
}