package flink.sql.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * ★应用场景：周期内累计PV，UV指标（如每天累计到当前这一分钟的PV，UV）。这类指标是一段周期内的累计
*状态，对分析师来说更具统计分析价值，而且几乎所有的复合指标都是基于此类指标的统计（不然离线为啥都要累计
*-天的数据，而不要一分钟累计的数据呢）。
  *★实际案例：每天的截止当前分钟的累计money（sum（money）），去重id数（count（distinctid））。每天代表渐
*讲式窗口大小为1天分钟代表渐进式窗口移动步长为分钟级别举例如下
 *
 *示例数据
 *time                      id                money
 *2021-11-01 00:01:00       A                  3
 *
 *可以看到，其特点就在于，每一分钟的输出结果都是当天零点累计到当前的结果
 *渐进式窗口目前只有WindowingTVF方案支持：
 *WindowingTVF方案（1.13只支持Streaming任务）
 */
object Cumulate {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    val sql1= "CREATE TABLE source_table (\n" +
      "user_id BIGINT,\n" +
      "money BIGINT,\n" +
      "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3))," +
      "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND)" +
      "WITH (" +
      "'connector' = 'datagen'," +
      "'rows-per-second' = '10'," +
      "'fields.user_id.min' = '1'," +
      "'fields.user_id.max' = '100000'," +
      "'fields.money.min' = '1'," +
      "'fields.money.max' = '100000')"

    val sql2 = "CREATE TABLE sink_table (\n" +
      "window_start TIMESTAMP(3),\n" +
      "window_end TIMESTAMP(3),\n" +
      "sum_money BIGINT,\n" +
      "count_distinct_id bigint) " +
      "WITH ('connector' = 'print')"

    val sql3 = "insert into sink_table\n" +
      "SELECT \n" +
      "window_start,\n" +
      "window_end,\n" +
      "sum(money) as sum_money,\n" +
      "count(distinct user_id) as count_distinct_id\n" +
      "FROM TABLE(CUMULATE(TABLE source_table,DESCRIPTOR(row_time),INTERVAL '20' SECOND,INTERVAL '1' DAY))\n" +
      "GROUP BY window_start,window_end"

    tableEnv.executeSql(sql1)
    tableEnv.executeSql(sql2)
    tableEnv.executeSql(sql3)

    /**
     * 示例结果展示
     * +I[2022-08-19T00:00, 2022-08-19T14:56:20, 4287045, 84]
       +I[2022-08-19T00:00, 2022-08-19T14:56:40, 16149885, 319]
       +I[2022-08-19T00:00, 2022-08-19T14:57, 28238597, 558]
       +I[2022-08-19T00:00, 2022-08-19T14:57:20, 40294287, 797]
       +I[2022-08-19T00:00, 2022-08-19T14:57:40, 52331576, 1036]
       +I[2022-08-19T00:00, 2022-08-19T14:58, 63941681, 1276]
     */
  }
}
