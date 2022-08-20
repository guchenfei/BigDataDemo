package flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 支持Batch/streaming 实时任务中一般用的非常少
 */
object OrderBy {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    val sql = "CREATE TABLE source_table_1 (\n" +
      "user_id BIGINT NOT NULL,\n" +
      "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
      "WATERMARK FOR row_time AS row_time)\n" +
      "WITH ('connector' = 'datagen'," +
      "'rows-per-second' = '10'," +
      "'fields.user_id.min' = '1'," +
      "'fields.user_id.max' = '10')"

    val sql2 = "CREATE TABLE sink_table (\n" +
      "user_id BIGINT) " +
      "WITH ('connector' = 'print')"

    val sql3 = "INSERT INTO sink_table\n" +
      "SELECT user_id\n" +
      "FROM source_table_1\n" +
      "Order By row_time, user_id desc"

    //根据结果打印来看,不断的查询对应的字段并且按照指定方式排序,每次查询后插入的是新的全量数据

    tableEnv.executeSql(sql)
    tableEnv.executeSql(sql2)
    tableEnv.executeSql(sql3)
  }
}
