package flink.sql.udf

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.api._

/**
 * SQL自定义函数（UDF）
 * Flink体系也提供了类似于其他大数据引擎的UDF体系
 * 自定义函数（UDF）是一种扩展开发机制，可以用来在查询语句里调用难以用SQL进行直接表达的频繁使用或自定义的
 * 逻辑。
 * 目前Flink自定义函数可以基于JVM语言（例如Java或Scala）或Python实现，实现者可以在UDF中使用任意第三方
 * 库，本章聚焦于使用Java语言开发自定义函数。
 * 当前Flink提供了一下几种UDF能力
 * 1.标量函数（Scalarfunctions或UDAF）：输入一条输出一条，将标量值转换成一个新标量值，对标Hive中的
 * UDF；
 * 2.表值函数（Tablefunctions或UDTF）：输入一条条输出多条，对标Hive中的UDTF
 * 3.聚合函数（Aggregatefunctions或UDAF）：输入多条输出一条，对标Hive中的UDAF；
 * 4.表值聚合函数（Tableaggregatefunctions或UDTAF）：仅仅支持TableAPl，不支持SQLAPI，其可以将多行转
 * 为多行；
 * 5.异步表值函数（Asynctablefunctions）：这是一种特殊的UDF，支持异步查询外部数据系统，用在前文介绍到的
 * ookupjoin中作为查询外部系统的函数。
 * 先直接给一个案例看看，怎么创建并在FlinkSQL中使用一个UDF
 */
object UDFTest {
  class SubStringFunction extends ScalarFunction{
    //构造方法入参需要实现序列化
    def eval(s:String,begin:Integer,end:Integer): String ={
      return s.substring(begin,end)
    }
  }

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //在Table Api 中可以直接引用class方式使用UDF
//    tableEnv.from("MyTable").select(call(classOf[SubstringFunction],$("myField"),5,12))
    //注册UDF
    tableEnv.createTemporarySystemFunction("SubStringFunction",classOf[SubStringFunction])
    //Table Api 调用UDF
    tableEnv.from("MyTable").select(call("SubStringFunction",$"myField",5,12))
    //Sql api 调用UDF
    tableEnv.sqlQuery("select SubStringFunction(myField,5,12) from MyTable")
  }
}
