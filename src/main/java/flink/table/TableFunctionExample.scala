package flink.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.Expressions.call
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row
/**
 * 5.3 表函数（Table Functions）
与用户定义的标量函数类似，用户定义的表函数，可以将0、1或多个标量值作为输入参数；与标量函数不同的是，它可以返回任意数量的行作为输出，而不是单个值。
为了定义一个表函数，必须扩展org.apache.flink.table.functions中的基类TableFunction并实现（一个或多个）求值方法。表函数的行为由其求值方法决定，求值方法必须是public的，并命名为eval。求值方法的参数类型，决定表函数的所有有效参数。
返回表的类型由TableFunction的泛型类型确定。求值方法使用protected collect(T)方法发出输出行。
在Table API中，Table函数需要与.joinLateral或.leftOuterJoinLateral一起使用。
joinLateral算子，会将外部表中的每一行，与表函数（TableFunction，算子的参数是它的表达式）计算得到的所有行连接起来。
而leftOuterJoinLateral算子，则是左外连接，它同样会将外部表中的每一行与表函数计算生成的所有行连接起来；并且，对于表函数返回的是空表的外部行，也要保留下来。
在SQL中，则需要使用Lateral Table（），或者带有ON TRUE条件的左连接。
下面的代码中，我们将定义一个表函数，在表环境中注册它，并在查询中调用它。
自定义TableFunction：
 */
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
