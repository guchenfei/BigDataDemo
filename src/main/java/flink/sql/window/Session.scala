package flink.sql.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 实际案例：计算每个用户在活跃期间（一个Session）总共购买的商品数量，如果用户5分钟没有活动则视为
 * Session 断开
 * 目前1.13版本中FlinkSQL不支持Session窗口的WindowTVF，所以这里就只介绍GroupWindowAggregation方案
 * GroupWindowAggregation方案【支持Batch/Streaming任务）
 */
object Session {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    val sql1 = "CREATE TABLE source_table (" +
      "dim STRING,\n    " +
      "user_id BIGINT,\n" +
      "price BIGINT,\n" +
      "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
      "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n) " +
      "WITH ('connector' = 'datagen',\n" +
      "'rows-per-second' = '10',\n" +
      "'fields.dim.length' = '1',\n" +
      "'fields.user_id.min' = '1',\n" +
      "'fields.user_id.max' = '100000',\n" +
      "'fields.price.min' = '1',\n" +
      "'fields.price.max' = '100000'\n)"

    val sql2 = "CREATE TABLE sink_table (\n" +
      "dim STRING,\n    " +
      "pv BIGINT,\n" +
      "window_start bigint) " +
      "WITH ('connector' = 'print')"

    val sql3 = "insert into sink_table\n" +
      "SELECT dim," +
      "count(1) as pv,\n" +
      "UNIX_TIMESTAMP(CAST(session_start(row_time, interval '5' minute) AS STRING)) * 1000 as window_start\n" +
    "FROM source_table\n" +
      "GROUP BY dim,session(row_time, interval '5' minute)"

    tableEnv.executeSql(sql1)
    tableEnv.executeSql(sql2)
    tableEnv.executeSql(sql3)
  }
}
