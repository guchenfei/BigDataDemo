package flink.sql.join

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 1.Regular Join定义（支持Batch＼Streaming）：Regular Join 其实就是和离线Hive SQL一样的 Regular Join， 通过条件关联两条流数据输出。
 * 2.应用场景：Join其实在我们的数仓建设过程中应用是非常广泛的。离线数仓可以说基本上是离不开Join的。那么实时数仓的建设也必然离不开Join，比如日志关联扩充维度数据，构建宽表；日志通过ID关联计算CTR。
 * 3.Regular Join包含以下几种（以L作为左流中的数据标识，R作为右流中的数据标识）：Inner Join（Inner Equal Join）：流任务中，只有两条流Join到才输出，输出＋［L，R］
 * Left Join（Outer Equal Join）：流任务中，左流数据到达之后，无论有没有Join到右流的数据，都会输出（Join到输出＋［L，R］，没Join到输出＋［L，null］），如果右流之后数据到达之后，发现左流之前输出过没有Join到的数据，则会发起回撤流，先输出—［L，null］，然后输出＋［L，R］
 * Right Join（Outer Equal Join）：有Left Join一样，左表和右表的执行逻辑完全相反
 * Full Join（Outer Equal Join）：流任务中，左流或者右流的数据到达之后，无论有没有Join到另外一条流的数据，都会输出（对右流来说：Join到输出＋［L，R］，没Join到输出＋［null，R］；对左流来说：Join到输出＋［L，R］，没Join到输出＋［L，null］）。如果一条流的数据到达之后，发现之前另一条流之前输出过没有Join到的数据，则会发起回撤流（左流数据到达为例：回撤—［null，R］，输出＋［L，R］，右流数据到达为例：回撤—［L，null］，输出＋［L，R］）。
 * 4.实际案例：案例为曝光日志关联点击日志筛选既有曝光又有点击的数据，并且补充点击的扩展参数（show innerclick): */

object RegularJoin {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //Inner Join
    val sql = "CREATE TABLE show_log_table (\n" +
      "log_id BIGINT,\n" +
      "show_params STRING\n)" +
      "WITH ('connector' = 'datagen'," +
      "'rows-per-second' = '2'," +
      "'fields.show_params.length' = '1'," +
      "'fields.log_id.min' = '1'," +
      "'fields.log_id.max' = '100')"

    val sql2 = "CREATE TABLE click_log_table (\n" +
      "log_id BIGINT,\n" +
      "click_params STRING)\n" +
      "WITH ('connector' = 'datagen'," +
      "'rows-per-second' = '2'," +
      "'fields.click_params.length' = '1'," +
      "'fields.log_id.min' = '1'," +
      "'fields.log_id.max' = '10')"

    val sql3 = "CREATE TABLE sink_table (" +
      "s_id BIGINT," +
      "s_params STRING," +
      "c_id BIGINT," +
      "c_params STRING)\n" +
      "WITH ('connector' = 'print')"

    val sql4 = "INSERT INTO sink_table\n" +
      "SELECT show_log_table.log_id as s_id," +
      "show_log_table.show_params as s_params," +
      "click_log_table.log_id as c_id," +
      "click_log_table.click_params as c_params\n" +
      "FROM show_log_table\n" +
      "INNER JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id"

    //Left Join
    val sql5 = "INSERT INTO sink_table\n" +
      "SELECT show_log_table.log_id as s_id," +
      "show_log_table.show_params as s_params," +
      "click_log_table.log_id as c_id," +
      "click_log_table.click_params as c_params\n" +
      "FROM show_log_table\n" +
      "LEFT JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id"

    //Full Join
    val sql6 = "INSERT INTO sink_table\n" +
      "SELECT show_log_table.log_id as s_id," +
      "show_log_table.show_params as s_params," +
      "click_log_table.log_id as c_id," +
      "click_log_table.click_params as c_params\n" +
      "FROM show_log_table\n" +
      "FULL JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id"

    tableEnv.executeSql(sql)
    tableEnv.executeSql(sql2)
    tableEnv.executeSql(sql3)
    tableEnv.executeSql(sql6)
  }
}
