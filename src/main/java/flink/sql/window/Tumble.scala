package flink.sql.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 关于滚动窗口有两种方式
 * 1.Group Window Aggregation(1.13版本之前只有该方案,1.13后标记废弃)
 * 2.Windowing TVF(1.13及之后建议使用,但是1.13只支持Streaming任务)
 */
object Tumble {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //1.Group Window Aggregation(1.13版本之前只有该方案,1.13后标记废弃)
    val sql = "CREATE TABLE source_table (" +
      "dim STRING," +
      "user_id BIGINT," +
      "price BIGINT," +
      "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3))," +
      "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND) " +
      "WITH (" +
      "'connector' = 'datagen'," +
      "'rows-per-second' = '10'," +
      "'fields.dim.length' = '1'," +
      "'fields.user_id.min' = '1'," +
      "'fields.user_id.max' = '100000'," +
      "'fields.price.min' = '1'," +
      "'fields.price.max' = '100000')";

    val sql2 = "CREATE TABLE sink_table (" +
      "dim STRING," +
      "pv BIGINT," +
      "sum_price BIGINT," +
      "max_price BIGINT," +
      "min_price BIGINT," +
      "uv BIGINT," +
      "window_start bigint)" +
      "WITH ('connector' = 'print')";

    val sql3 = "insert into sink_table\n" +
      "select dim," +
      "count(*) as pv," +
      "sum(price) as sum_price," +
      "max(price) as max_price," +
      "min(price) as min_price," +
      "count(distinct user_id) as uv," +
      "UNIX_TIMESTAMP(CAST(tumble_start(row_time, interval '1' minute) AS STRING)) * 1000  as window_start\n" +
      "from source_table\n" +
      "group by dim,tumble(row_time, interval '1' minute)";

    val sql4 = "CREATE TABLE source_table (\n" +
      "dim STRING,\n" +
      "user_id BIGINT,\n" +
      "price BIGINT,\n" +
      "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n   " +
      "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n) " +
      "WITH (\n  " +
      "'connector' = 'datagen',\n " +
      "'rows-per-second' = '10',\n" +
      "'fields.dim.length' = '1',\n" +
      "'fields.user_id.min' = '1',\n " +
      "'fields.user_id.max' = '100000',\n " +
      "'fields.price.min' = '1',\n " +
      "'fields.price.max' = '100000')"

    val sql5 = "CREATE TABLE sink_table (\n" +
      "dim STRING,\n" +
      "pv BIGINT,\n" +
      "sum_price BIGINT,\n" +
      " max_price BIGINT,\n" +
      "min_price BIGINT,\n" +
      "uv BIGINT,\n" +
      "window_start bigint\n) " +
      "WITH (" +
      "'connector' = 'print')"

    val sql6 = "insert into sink_table\n   " +
      "SELECT \n" +
      "dim,\n" +
      "count(*) as pv,\n     " +
      "sum(price) as sum_price,\n    " +
      "max(price) as max_price,\n     " +
      "min(price) as min_price,\n     " +
      "count(distinct user_id) as uv,\n     " +
      "UNIX_TIMESTAMP(CAST(window_start AS STRING)) * 1000 as window_start\n" +
    "FROM TABLE(" +
      "TUMBLE(TABLE source_table,DESCRIPTOR(row_time),INTERVAL '60' SECOND))\n" +
      "GROUP BY window_start,window_end,dim"

    tableEnv.executeSql(sql4)
    tableEnv.executeSql(sql5)
    tableEnv.executeSql(sql6)
  }
}
