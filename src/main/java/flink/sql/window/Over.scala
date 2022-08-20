package flink.sql.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 *
 * DML:Over聚合
 * Over聚合定义{支batch\streaming)：可以理解为是一柙殊的滑动窗囗聚合函数。
 * 那这里我们孚Over聚合与窗口聚台一个比，其之间的最大不同之处在于、
 * 窗囗聚合、不在group by中的字段，不能直接在select中拿到
 * Over聚合：能够保留原始字段
 * 注意
 * 其实在生产环境中，Over聚合的使用场景还是比较少的。在Hive中也有相同的聚合，但是小伙伴咱可以想想你在离线
 * 数仓经常使用吗？
 *
 * 应用场景:计算最近一段滑动窗囗的聚合结果数据。
 * 实际案例:查询每个产品最近一小时订单的金额总和。
 */
object Over {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    val sql = "SELECT order_id," +
      "order_time, amount, " +
      "SUM(amount) OVER (PARTITION BY product ORDER BY order_time RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) AS one_hour_prod_amount_sum\n" +
      "FROM Orders"

    /**
     * 语法格式如下
     * SELECT
     * agg_func(agg_col) OVER (
     * [PARTITION BY col1[, col2, ...]]
     * ORDER BY time_col
     * range_definition),
     * ...
     * FROM ...
     *
     *
     * order by 必须EventTime or ProcessTime
     * PARTITION BY 聚合粒度
     * range_definition 聚合窗口的聚合数据范围 1:行聚合,2:时间区间聚合
     *
     * 语法特征:单行可以和聚合值进行拼接,其他语法不支持的只有Group by聚合才能匹配聚合函数
     */

    //时间区间聚合
    val sql2 = "CREATE TABLE source_table (\n" +
      "order_id BIGINT,\n" +
      "product BIGINT,\n" +
      "amount BIGINT,\n" +
      "order_time as cast(CURRENT_TIMESTAMP as TIMESTAMP(3)),\n" +
      "WATERMARK FOR order_time AS order_time - INTERVAL '0.001' SECOND)\n" +
      "WITH ('connector' = 'datagen'," +
      "'rows-per-second' = '1',\n" +
      "'fields.order_id.min' = '1',\n" +
      "'fields.order_id.max' = '2',\n" +
      "'fields.amount.min' = '1',\n" +
      "'fields.amount.max' = '10',\n" +
      "'fields.product.min' = '1',\n" +
      "'fields.product.max' = '2'\n)"

    val sql3 = "CREATE TABLE sink_table (\n" +
      "product BIGINT,\n" +
      "order_time TIMESTAMP(3),\n" +
      "amount BIGINT,\n" +
      "one_hour_prod_amount_sum BIGINT\n) " +
      "WITH ('connector' = 'print')"

    val sql4 = "INSERT INTO sink_table\n" +
      "SELECT product, order_time, amount," +
      "SUM(amount) OVER (PARTITION BY product ORDER BY order_time RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) AS one_hour_prod_amount_sum\n" +
      "FROM source_table"

    //行聚合
    val sql5 = "CREATE TABLE source_table (\n" +
      "order_id BIGINT,\n" +
      "product BIGINT,\n" +
      "amount BIGINT,\n" +
      "order_time as cast(CURRENT_TIMESTAMP as TIMESTAMP(3)),\n" +
      "WATERMARK FOR order_time AS order_time - INTERVAL '0.001' SECOND)\n" +
      "WITH ('connector' = 'datagen'," +
      "'rows-per-second' = '1'," +
      "'fields.order_id.min' = '1'," +
      "'fields.order_id.max' = '2'," +
      "'fields.amount.min' = '1'," +
      "'fields.amount.max' = '2'," +
      "'fields.product.min' = '1'," +
      "'fields.product.max' = '2')"


    val sql6 = "CREATE TABLE sink_table (\n" +
      "product BIGINT,\n" +
      "order_time TIMESTAMP(3),\n" +
      "amount BIGINT,\n" +
      "one_hour_prod_amount_sum BIGINT)\n" +
      "WITH ('connector' = 'print')"

    val sql7 = "INSERT INTO sink_table\n" +
      "SELECT product, order_time, amount," +
      "SUM(amount) OVER (PARTITION BY product ORDER BY order_time ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS one_hour_prod_amount_sum\n" +
      "FROM source_table"

    /**
     * 如果业务中存在多个聚合开窗场景,可以采用Flink Sql简化语法
     *
     * SELECT order_id, order_time, amount,
     * SUM(amount) OVER w AS sum_amount,
     * AVG(amount) OVER w AS avg_amount
     * FROM Orders
     * -- 使用下面子句，定义 Over Window
     * WINDOW w AS (
     * PARTITION BY product
     * ORDER BY order_time
     * RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)
     */

    tableEnv.executeSql(sql5)
    tableEnv.executeSql(sql6)
    tableEnv.executeSql(sql7)
  }
}
