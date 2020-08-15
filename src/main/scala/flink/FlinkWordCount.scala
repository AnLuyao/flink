package flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
//隐式类型转换
import org.apache.flink.api.scala._
/**
  * Created by anluyao on 2019-09-06 00:06
  */
object FlinkWordCount {
  def main(args: Array[String]): Unit = {
//    val tool: ParameterTool = ParameterTool.fromArgs(args)
//    val inputPath: String = tool.get("input")
//    val outPath: String = tool.get("output")
    val env = ExecutionEnvironment.createLocalEnvironment(1)
    val text = env.readTextFile("/Users/anluyao/test/sparkwordCount.txt")
    val counts = text.flatMap(_.toLowerCase.split(",").filter(_.nonEmpty))
      .map{x=>(x,1)}
      .groupBy(0)
      .sum(1)

    counts.print()
    //输出结果到外部文件
    counts.writeAsText("/Users/anluyao/test/output.log",WriteMode.OVERWRITE).setParallelism(1)
    env.execute("FlinkWordCount")
  }

}
