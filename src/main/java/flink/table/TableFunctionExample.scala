package flink.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.Expressions.call
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .fromElements(
        "hello#world",
        "atguigu#bigdata"
      )

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.createTemporaryView("MyTable", stream, $"s")

    // 注册函数
    tEnv.createTemporarySystemFunction("SplitFunction", classOf[SplitFunction])

    // 在Table API里调用注册好的函数
    tEnv
      .from("MyTable")
      .joinLateral(call("SplitFunction", $"s"))
      .select($"s", $"word", $"length")
      .toAppendStream[Row]
      .print()

    tEnv
      .from("MyTable")
      .leftOuterJoinLateral(call("SplitFunction", $"s"))
      .select($"s", $"word", $"length")

    // 在 SQL 里调用注册好的函数
    //直接用表进行拼接视图
    tEnv.sqlQuery(
      "SELECT s, word, length " +
        "FROM MyTable, LATERAL TABLE(SplitFunction(s))")

    //通过左外连接方式拼接视图
    tEnv.sqlQuery(
      "SELECT s, word, length " +
        "FROM MyTable " +
        "LEFT JOIN LATERAL TABLE(SplitFunction(s)) ON TRUE")

    env.execute()
  }

  @FunctionHint(output = new DataTypeHint("ROW<word STRING, length INT>"))
  class SplitFunction extends TableFunction[Row] {
    def eval(str: String): Unit = {
      // use collect(...) to emit a row
      str.split("#").foreach(s => collect(Row.of(s, Int.box(s.length))))
    }
  }
}
