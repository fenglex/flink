package ink.haifeng.stream

import akka.stream.actor.WatermarkRequestStrategy
import cn.hutool.db.Db
import cn.hutool.db.ds.simple.SimpleDataSource
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import ink.haifeng.VisitLog
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConverters._

object BatchStream {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val param: ParameterTool = ParameterTool.fromArgs(args)
    val stream = env.addSource(new MysqlSource(param))
    stream.print()
    env.execute()
  }
}

private class MysqlSource(param: ParameterTool) extends RichSourceFunction[VisitLog] {

  var db: Db = _

  override def open(parameters: Configuration): Unit = {
    val url = param.get("url", "jdbc:mysql://192.168.31.88:3306/db_flink")
    val user = param.get("user", "root")
    val pass = param.get("pass", "haifeng123")
    val source = new SimpleDataSource(url, user, pass)
    db = Db.use(source)
  }

  override def run(sourceContext: SourceFunction.SourceContext[VisitLog]): Unit = {
    val list = db.query("select * from tb_visit_log order by visit_time asc")
    list.asScala.map(e => {
      VisitLog(e.getStr("uid"), e.getStr("device"), e.getInt("visit_time"))
    }).foreach(e => {
      sourceContext.collect(e)
    })

  }

  override def cancel(): Unit = {

  }
}