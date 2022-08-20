package flink.sql.join

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 1.Temporal Join定义（支持Batch＼Streaming）: Temporal Join 在离线的概念中其实是没有类似的Join 概念 的，但是离线中常常会维护一种表叫做拉链快照表，使用一个明细表去join这个拉链快照表的join方式就叫做Temporal Join。而Flink SQL中也有对应的概念，表叫做 Versioned Table，使用一个明细表去 join 这个 Versioned Table的 join 操作就叫做 Temporal Join。Temporal Join中，Versioned Table 其实就是对同一条key（在DDL中 以primary key 标记同一个key）的历史版本（根据时间划分版本）做一个维护，当有明细表Join这个表时，可以根据明细表中的时间版本选择 Versioned Table对应时间区间内的快照数据进行join。
 * 2.应用场景：比如常见的汇率数据（实时的根据汇率计算总金额），在12：00之前（事件时间），人民币和美元汇率是7：1，在12：00之后变为6：1，那么在12：00之前数据就要按照7：1进行计算，12：00之后就要按照6：1计算。在事件时间语义的任务中，事件时间12：00之前的数据，要按照7：1进行计算，12：00之后的数据，要按照6：1 进行计算。这其实就是离线中快照的概念，维护具体汇率的表在Flink SQL体系中就叫做 Versioned Table。
 * 3.Versioned Table：Versioned Table 中存储的数据通常是来源于CDC或者会发生更新的数据。Flink SQL会为 Versioned Table 维护 Primary Key下的所有历史时间版本的数据。举一个汇率的场景的案例来看一下一个
 * Versioned Table的两种定义方式。
 *
 * (1)PRIMARY KEY 定义方式：
 * -- 定义一个汇率 versioned 表，其中 versioned 表的概念下文会介绍到
 * CREATE TABLE currency_rates (
 * currency STRING,
 * conversion_rate DECIMAL(32, 2),
 * update_time TIMESTAMP(3) METADATA FROM `values.source.timestamp` VIRTUAL,
 * WATERMARK FOR update_time AS update_time,
 * -- PRIMARY KEY 定义方式
 * PRIMARY KEY(currency) NOT ENFORCED
 * ) WITH (
 * 'connector' = 'kafka',
 * 'value.format' = 'debezium-json',
 * /* ... */
 * );
 *
 * (2)Deduplicate定义方式
 * -- 定义一个 append-only 的数据源表
 * CREATE TABLE currency_rates (
 * currency STRING,
 * conversion_rate DECIMAL(32, 2),
 * update_time TIMESTAMP(3) METADATA FROM `values.source.timestamp` VIRTUAL,
 * WATERMARK FOR update_time AS update_time
 * ) WITH (
 * 'connector' = 'kafka',
 * 'value.format' = 'debezium-json',
 * /* ... */
 * );
 *
 * -- 将数据源表按照 Deduplicate 方式定义为 Versioned Table
 * CREATE VIEW versioned_rates AS
 * SELECT currency, conversion_rate, update_time   -- 1. 定义 `update_time` 为时间字段
 * FROM (
 * SELECT *,
 * ROW_NUMBER() OVER (PARTITION BY currency  -- 2. 定义 `currency` 为主键
 * ORDER BY update_time DESC              -- 3. ORDER BY 中必须是时间戳列
 * ) AS rownum
 * FROM currency_rates)
 * WHERE rownum = 1;
 *
 * Temporal Join 支持的时间语义：事件时间,处理时间
 * 案例:汇率计算
 */
object TemporalJoin {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //EventTime
    //-- 1. 定义一个输入订单表
    val sql = "CREATE TABLE orders (\n" +
      "order_id STRING,\n" +
      "price DECIMAL(32,2),\n" +
      "currency STRING,\n" +
      "order_time  TIMESTAMP(3),\n" +
      "WATERMARK FOR order_time AS order_time)\n" +
      "WITH (/* ... */)"

    //-- 2. 定义一个汇率 versioned 表
    val sql2 = "CREATE TABLE currency_rates (\n" +
      "currency STRING,\n" +
      "conversion_rate DECIMAL(32, 2),\n" +
      "update_time TIMESTAMP(3) METADATA FROM `values.source.timestamp` VIRTUAL,\n" +
      "WATERMARK FOR update_time AS update_time,\n" +
      "PRIMARY KEY(currency) NOT ENFORCED)\n" +
      "WITH ('connector' = 'kafka'," +
      "'value.format' = 'debezium-json'," +
      "/* ... */)"

    //-- 3. Temporal Join 逻辑
    //-- SQL 语法为：FOR SYSTEM_TIME AS OF
    val sql3 = "SELECT " +
      "order_id,\n" +
      "price,\n" +
      "currency,\n" +
      "conversion_rate,\n" +
      "order_time\n" +
      "FROM orders\n" +
      "LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF orders.order_time ON orders.currency = currency_rates.currency"

    //示例:结果
    /**
     * order_id  price  货币       汇率             order_time
     * ========  =====  ========  ===============  =========
     * o_001     11.11  EUR       1.14             12:00:00
     * o_002     12.51  EUR       1.10             12:06:00
     *
     *
     * 注意．
     * 1.事件时间的TemporalJoin一定要结左右两张表都设WaterMark
     * 2.事件时间的TemporalJoin一定要把VersionedTable的主键包含在JOIN ON的条件中.
     */


    //Process Time
    /**
     * 10:15> SELECT * FROM LatestRates;
     *
     * currency   rate
     * ======== ======
     * US Dollar   102
     * Euro        114
     * Yen           1
     *
     * 10:30> SELECT * FROM LatestRates;
     *
     * currency   rate
     * ======== ======
     * US Dollar   102
     * Euro        114
     * Yen           1
     *
     * -- 10:42 时，Euro 的汇率从 114 变为 116
     * 10:52> SELECT * FROM LatestRates;
     *
     * currency   rate
     * ======== ======
     * US Dollar   102
     * Euro        116     <==== 从 114 变为 116
     * Yen           1
     *
     * -- 从 Orders 表查询数据
     * SELECT * FROM Orders;
     *
     * amount currency
     * ====== =========
     * 2 Euro             <== 在处理时间 10:15 到达的一条数据
     * 1 US Dollar        <== 在处理时间 10:30 到达的一条数据
     * 2 Euro             <== 在处理时间 10:52 到达的一条数据
     *
     * -- 执行关联查询
     * SELECT
     * o.amount, o.currency, r.rate, o.amount * r.rate
     * FROM
     * Orders AS o
     * JOIN LatestRates FOR SYSTEM_TIME AS OF o.proctime AS r
     * ON r.currency = o.currency
     *
     * -- 结果如下：
     * amount currency     rate   amount*rate
     * ====== ========= ======= ============
     * 2 Euro          114          228    <== 在处理时间 10:15 到达的一条数据
     * 1 US Dollar     102          102    <== 在处理时间 10:30 到达的一条数据
     * 2 Euro          116          232    <== 在处理时间 10:52 到达的一条数据
     */

    //根据解释
    //在EventTime 下 FlinkSql会保留任何时间的版本数据
    //在ProcessTime下只保留了当前时间最新的版本数据,其他历史版本数据毫无意义(我理解ProcessTime只处理当下时间)
  }
}
