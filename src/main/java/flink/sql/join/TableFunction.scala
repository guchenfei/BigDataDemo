package flink.sql.join

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction

/**
 * TableFunction（自定义列转行）
 * 应用场景（支持batch\streaming)这个其实和ArrayExpansion功能类似，但是TableFunction本质上是
 * 个UDTF函数，和离线SQL一样，我们可以自定义UDTF去决定列转行的逻辑
 * TableFunction使用分类
 *
 * InnerJoinTableFunction:如果UDTF返回结果为空，则相当于1行转为0行，这行数据直接被丢弃
 * LeftJoinTableFunction:如果UDTF返回结果为空，这行数据不会被丢弃，只会在结果中填充null值
 */
object TableFunction {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    val sql = "CREATE FUNCTION user_profile_table_func AS 'flink.sql.join.TableFunction$UserProfileTableFunction'"
    val sql2 = "CREATE TABLE source_table (\n" +
      "    user_id BIGINT NOT NULL,\n" +
      "    name STRING,\n" +
      "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
      "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
      ") WITH (\n" +
      "  'connector' = 'datagen',\n" +
      "  'rows-per-second' = '10',\n" +
      "  'fields.name.length' = '1',\n" +
      "  'fields.user_id.min' = '1',\n" +
      "  'fields.user_id.max' = '10')"

      val sql3 = "CREATE TABLE sink_table (\n" +
      "    user_id BIGINT,\n" +
      "    name STRING,\n" +
      "    age INT,\n" +
      "    row_time TIMESTAMP(3)\n" +
      ") WITH ('connector' = 'print')"

    val sql4 = "INSERT INTO sink_table\n" +
      "SELECT user_id,\n" +
      "       name,\n" +
      "       age,\n" +
      "       row_time\n" +
      "FROM source_table,\n" +
      // Table Function Join 语法对应 LATERAL TABLE
      "LATERAL TABLE(user_profile_table_func(user_id)) t(age)";

    tableEnv.executeSql(sql)
    tableEnv.executeSql(sql2)
    tableEnv.executeSql(sql3)
    tableEnv.executeSql(sql4)
  }

  class UserProfileTableFunction extends TableFunction[Integer] {
    def eval(userId: Long): Unit = { // 自定义输出逻辑
      if (userId <= 5) { // 一行转 1 行
        collect(1)
      } else { // 一行转 3 行
        collect(1)
        collect(2)
        collect(3)
      }
    }
  }
}
