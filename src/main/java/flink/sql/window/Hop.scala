package flink.sql.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 *★实际案例：简单且常见的分维度分钟级别同时在线用户数，1分钟输出一次，计算最近5分钟的数据
依然是GroupWindowAggregation、WindowingTVF两种方案：
★GroupWindowAggregation方案（支持Batch\Streaming任务）
★WindowingTVF方案（1.13只支持Streaming任务）
 */
object Hop {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //GroupWindowAggregation方案（支持Batch\Streaming任务）
    val sql1 = "CREATE TABLE source_table (\n  " +
      "dim STRING,\n" +
      "user_id BIGINT,\n" +
      "price BIGINT,\n" +
      "-- 事件时间戳\n" +
      "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
      "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n)" +
      "WITH ('connector' = 'datagen',\n" +
      "'rows-per-second' = '10',\n" +
      "'fields.dim.length' = '1',\n" +
      "'fields.user_id.min' = '1',\n" +
      "'fields.user_id.max' = '100000',\n" +
      "'fields.price.min' = '1',\n" +
      "'fields.price.max' = '100000'\n)"

    val sql2 = "CREATE TABLE sink_table (\n  " +
      "dim STRING,\n" +
      "uv BIGINT,\n " +
      "window_start bigint\n)" +
      "WITH ('connector' = 'print')"

    val sql3 = "insert into sink_table\n" +
      "SELECT dim," +
      "count(distinct user_id) as uv,\n" +
      "UNIX_TIMESTAMP(CAST(hop_start(row_time, interval '1' minute, interval '5' minute) AS STRING)) * 1000 as window_start\n" +
    "FROM source_table\n" +
      "GROUP BY dim,hop(row_time, interval '1' minute, interval '5' minute)"

    //WindowingTVF方案（1.13只支持Streaming任务）
    val sql4 = "CREATE TABLE source_table (\n  " +
      "dim STRING,\n" +
      "user_id BIGINT,\n" +
      "price BIGINT,\n" +
      "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
      "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n) " +
      "WITH (" +
      "'connector' = 'datagen'," +
      "'rows-per-second' = '10',\n  " +
      "'fields.dim.length' = '1',\n " +
      "'fields.user_id.min' = '1',\n " +
      "'fields.user_id.max' = '100000',\n  " +
      "'fields.price.min' = '1',\n" +
      "'fields.price.max' = '100000'\n)"

    val sql5 = "CREATE TABLE sink_table (\n " +
      "dim STRING,\n" +
      "uv BIGINT,\n" +
      "window_start bigint\n) " +
      "WITH ('connector' = 'print')"

    val sql6 = "insert into sink_table\n" +
      "SELECT dim,\n" +
      "count(distinct user_id) as bucket_uv,\n" +
      "UNIX_TIMESTAMP(CAST(window_start AS STRING)) * 1000 as window_start\n" +
    "FROM TABLE(HOP(TABLE source_table,DESCRIPTOR(row_time), INTERVAL '1' MINUTES, INTERVAL '5' MINUTES))\n" +
      "GROUP BY window_start,window_end,dim"
    tableEnv.executeSql(sql4)
    tableEnv.executeSql(sql5)
    tableEnv.executeSql(sql6)
  }
}
