package flink.sql.join

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 *
 * 应用饧景（支持：将表中ARRAY类型字段拍平，转为多行
 * 实际案例：如某些场景下，日志是合并、攒批上报的，就可以便用这种方式将一个Array转为多行。
 */
object ArrayExpansion {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    val sql = "CREATE TABLE show_log_table (\n" +
      "log_id BIGINT,\n" +
      "show_params ARRAY<STRING>)\n" +
      "WITH ('connector' = 'datagen'," +
      "'rows-per-second' = '1'," +
      "'fields.log_id.min' = '1'," +
      "'fields.log_id.max' = '10')"


    val sql2 = "CREATE TABLE sink_table (" +
      "log_id BIGINT," +
      "show_param STRING)\n" +
      "WITH ('connector' = 'print')"

    //-- array 炸开语法
    val sql3 = "INSERT INTO sink_table\n" +
      "SELECT log_id,\n" +
      "t.show_param as show_param\n" +
      "FROM show_log_table\n" +
      "CROSS JOIN UNNEST(show_params) AS t (show_param)"

    /**
     * show_log_table原始数据
     * +I[7, [a, b, c]]
     * +I[5, [d, e, f]]
     *
     * 输出结果
     * -- +I[7, [a, b, c]] 一行转为 3 行
     * +I[7, a]
     * +I[7, b]
     * +I[7, c]
     * -- +I[5, [d, e, f]] 一行转为 3 行
     * +I[5, d]
     * +I[5, e]
     * +I[5, f]
     */

    tableEnv.executeSql(sql)
    tableEnv.executeSql(sql2)
    tableEnv.executeSql(sql3)
  }
}
