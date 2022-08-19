package flink.sql.window.cumulate

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object GroupingSets {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    val sql = "CREATE TABLE source_table (\n" +
      "age STRING,\n" +
      "sex STRING,\n" +
      "user_id BIGINT,\n" +
      "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
      "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND)\n" +
      "WITH (" +
      "'connector' = 'datagen'," +
      "'rows-per-second' = '1'," +
      "'fields.age.length' = '1'," +
      "'fields.sex.length' = '1'," +
      "'fields.user_id.min' = '1'," +
      "'fields.user_id.max' = '100000')"

    val sql2 = "CREATE TABLE sink_table (\n" +
      "age STRING,\n" +
      "sex STRING,\n" +
      "uv BIGINT,\n" +
      "window_end timestamp(3))\n" +
      "WITH ('connector' = 'print')"

    val sql3 = "insert into sink_table\n" +
      "SELECT \n " +
      "if (age is null, 'ALL', age) as age,\n" +
      "if (sex is null, 'ALL', sex) as sex,\n" +
      "count(distinct user_id) as bucket_uv,\n" +
      "window_end\n" +
      "FROM TABLE(CUMULATE(TABLE source_table,DESCRIPTOR(row_time), INTERVAL '5' SECOND,INTERVAL '1' DAY))\n" +
      "GROUP BY window_start,window_end,GROUPING SETS ((),(age),(sex),(age, sex))"

    /**
     * 我们如果在hive sql中写同样逻辑,必须指明group by子维度
     * insert into sink_table
     * SELECT
     * UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 as window_end,
     * if (age is null, 'ALL', age) as age,
     * if (sex is null, 'ALL', sex) as sex,
     * count(distinct user_id) as bucket_uv
     * FROM source_table
     * GROUP BY
     * age    ---子维度
     * , sex  ---子维度
     * -- hive sql grouping sets 写法
     * GROUPING SETS (
     * ()
     * , (age)
     * , (sex)
     * , (age, sex)
     * )
     */

    tableEnv.executeSql(sql)
    tableEnv.executeSql(sql2)
    tableEnv.executeSql(sql3)
  }
}
