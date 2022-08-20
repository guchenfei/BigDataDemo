package flink.sql.join

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 1.Interval Join定义（支持Batch＼Streaming）：Interval Join在离线的概念中是没有的。Interval Join可以让一 条流去 Join 另一条流中前后一段时间内的数据。
 * 2.应用场景：为什么有 Regular Join 还要 Interval Join呢？刚刚的案例也讲了，Regular Join会产生回撤流，但是 在实时数仓中一般写入的sink都是类似于Kafka这样的消息队列，然后后面接clickhouse等引擎，这些引擎又不具备处理回撤流的能力。所以博主理解 Interval Join就是用于消灭回撤流的。
 * 3.Interval Join 包含以下几种（以L作为左流中的数据标识，R作为右流中的数据标识）：
 *
 * Inner Interval Join：流任务中，只有两条流Join到（满足Join on中的条件：两条流的数据在时间区间＋满足其他等值条件）才输出，输出＋［L，R］
 * Left Interval Join：流任务中，左流数据到达之后，如果没有Join到右流的数据，就会等待（放在State 中等），如果之后右流之后数据到达之后，发现能和刚刚那条左流数据Join到，则会输出＋［L，R］。事件时间中随着Watermark的推进（也支持处理时间）。如果发现发现左流State中的数据过期了，就把左流中过期的数据从State 中删除，然后输出＋［L，null］，如果右流State中的数据过期了，就直接从State中删除。
 * Right Interval Join：和Left Interval Join 执行逻辑一样，只不过左表和右表的执行逻辑完全相反
 * Full Interval Join：流任务中，左流或者右流的数据到达之后，如果没有Join到另外一条流的数据，就会等待（左流放在左流对应的State中等，右流放在右流对应的State中等），如果之后另一条流数据到达之后，发现能和刚刚那条数据Join到，则会输出＋［L，R］。事件时间中随着Watermark的推进（也支持处理时间），发现State中的数据能够过期了，就将这些数据从State 中删除并且输出（左流过期输出＋［L，null］，右流过期输出—［null，R］）
 * 可以发现 Inner Interval Join 和其他三种 Outer Interval Join的区别在于，Outer在随着时间推移的过程中，如果有数据 过期了之后，会根据是否是Outer将没有Join到的数据也给输出。
 *
 * 4.实际案例：还是刚刚的案例，曝光日志关联点击日志筛选既有曝光又有点击的数据，条件是曝光关联之后发生4小时之内的点击，并且补充点击的扩展参数（show inner interval click）： */
object IntervalJoin {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    val sql = "CREATE TABLE show_log_table (\n" +
      "log_id BIGINT," +
      "show_params STRING," +
      "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3))," +
      "WATERMARK FOR row_time AS row_time)\n" +
      "WITH ('connector' = 'datagen'," +
      "'rows-per-second' = '1'," +
      "'fields.show_params.length' = '1'," +
      "'fields.log_id.min' = '1'," +
      "'fields.log_id.max' = '10')"

    val sql2 = "CREATE TABLE click_log_table (" +
      "log_id BIGINT," +
      "click_params STRING," +
      "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3))," +
      "WATERMARK FOR row_time AS row_time)\n" +
      "WITH ('connector' = 'datagen'," +
      "'rows-per-second' = '1'," +
      "'fields.click_params.length' = '1'," +
      "'fields.log_id.min' = '1'," +
      "'fields.log_id.max' = '10')"

    val sql3 = "CREATE TABLE sink_table (\n" +
      "s_id BIGINT," +
      "s_params STRING," +
      "c_id BIGINT," +
      "c_params STRING)\n " +
      "WITH ('connector' = 'print')"

    val sql4 = "INSERT INTO sink_table\n" +
      "SELECT show_log_table.log_id as s_id," +
      "show_log_table.show_params as s_params," +
      "click_log_table.log_id as c_id," +
      "click_log_table.click_params as c_params\n" +
      "FROM show_log_table INNER JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id\n" +
      "AND show_log_table.row_time BETWEEN click_log_table.row_time - INTERVAL '4' HOUR AND click_log_table.row_time"

    //Left Interval Join
    val sql5 = "INSERT INTO sink_table\n" +
      "SELECT show_log_table.log_id as s_id," +
      "show_log_table.show_params as s_params," +
      "click_log_table.log_id as c_id," +
      "click_log_table.click_params as c_params\n" +
      "FROM show_log_table LEFT JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id\n" +
      "AND show_log_table.row_time BETWEEN click_log_table.row_time - INTERVAL '5' SECOND AND click_log_table.row_time + INTERVAL '5' SECOND"

    tableEnv.executeSql(sql)
    tableEnv.executeSql(sql2)
    tableEnv.executeSql(sql3)
    tableEnv.executeSql(sql5)
  }
}
