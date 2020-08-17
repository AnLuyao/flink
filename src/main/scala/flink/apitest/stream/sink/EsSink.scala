package flink.apitest.stream.sink

import java.util

import flink.apitest.stream.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * Created by anluyao on 2020-08-04 21:45
  */
object EsSink {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val inputStream: DataStream[String] = environment.readTextFile("/Users/anluyao/workspace/bigdata-flink/src/main/resources/sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(x => {
      val dataArray: Array[String] = x.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    //定义一个httpHosts
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))
    //定义一个ElasticsearchSinkFunction
    val esSinkFunc: ElasticsearchSinkFunction[SensorReading] = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //包装写入es的数据
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("sensor_id", t.id)
        dataSource.put("sensor_temp", t.temperature.toString)
        dataSource.put("sensor_timestamp", t.timestamp.toString)
        //创建一个index request
        val indexRequest: IndexRequest = Requests.indexRequest()
          .index("sensor_temp")
          .`type`("readingdata")
          .source(dataSource)
        //用indexer发送HTTP请求
        requestIndexer.add(indexRequest)
        println(t + " saved successfuly")
      }
    }
    dataStream.addSink(new ElasticsearchSink.Builder[SensorReading](httpHosts, esSinkFunc).build())

    environment.execute("es sink test")
  }

}
