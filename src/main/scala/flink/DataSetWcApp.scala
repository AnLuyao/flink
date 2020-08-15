package flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * Created by anluyao on 2020-02-22 17:18
  * 批处理Word Count
  */
object DataSetWcApp {
  def main(args: Array[String]): Unit = {
    //1. env
    //2. source
    //3. transform
    //4. sink
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val inputPath: String = tool.get("input")
    val outputPath: String = tool.get("output")
    //创建一个批处理的执行环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val txtDataSet: DataSet[String] = environment.readTextFile(inputPath)
    val aggSet: AggregateDataSet[(String, Int)] = txtDataSet
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
    aggSet.print()
//    aggSet.writeAsCsv(outputPath).setParallelism(1)
//    environment.execute()
  }

}
