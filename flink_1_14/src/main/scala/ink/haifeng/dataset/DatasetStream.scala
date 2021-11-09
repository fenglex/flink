package ink.haifeng.dataset

import org.apache.flink.api.scala._

object DatasetStream {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val file: DataSet[String] = env.readTextFile("/Users/haifeng/workspace/flink/doc/tb_visit_log.csv")
    file.map(e=>(e.split(",")(1),1))
      .groupBy(0)
      .sum(1)
      .print()
    //env.execute("test")
  }

}
