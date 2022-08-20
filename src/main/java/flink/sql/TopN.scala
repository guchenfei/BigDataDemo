package flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 案例:取某个搜索关键词下的搜索热度前100的词条数据
 */
object TopN {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //    -- 字段名          备注
    //    -- key          搜索关键词
    //    -- name         搜索热度名称
    //    -- search_cnt     热搜消费热度（比如 3000）
    //    -- timestamp       消费词条时间戳
    val sql = "CREATE TABLE source_table (\n" +
      "name BIGINT NOT NULL,\n" +
      "search_cnt BIGINT NOT NULL,\n" +
      "key BIGINT NOT NULL,\n" +
      "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
      "WATERMARK FOR row_time AS row_time\n) " +
      "WITH (\n  ...\n)"

    //    -- 数据汇 schema
    //    -- key          搜索关键词
    //    -- name         搜索热度名称
    //    -- search_cnt     热搜消费热度（比如 3000）
    //    -- timestamp       消费词条时间戳
    val sql2 = " CREATE TABLE sink_table (\n" +
      "key BIGINT,\n" +
      "name BIGINT,\n" +
      "search_cnt BIGINT,\n" +
      "`timestamp` TIMESTAMP(3))\n " +
      "WITH (\n...\n)"

    //-- DML 逻辑
    val sql3 = "INSERT INTO sink_table\n" +
      "SELECT key, name, search_cnt, row_time as `timestamp`\n" +
      "FROM (" +
      "SELECT key, name, search_cnt, row_time, \n" +
      "-- 根据热搜关键词 key 作为 partition key，然后按照 search_cnt 倒排取前 100 名\n" +
      "ROW_NUMBER() OVER (PARTITION BY key ORDER BY search_cnt desc) AS rownum \n" +
      "FROM source_table)\n" +
      "WHERE rownum <= 100"

    /**
     * 示例结果
     * -D[关键词1, 词条1, 4944,时间戳]
     * +I[关键词1, 词条1, 8670,...]
     * +I[关键词1, 词条2, 1735,...]
     * -D[关键词1, 词条3, 6641,...]
     * +I[关键词1, 词条3, 6928,...]
     * -D[关键词1, 词条4, 6312,...]
     * +I[关键词1, 词条4, 7287,...]
     *
     * 存在回撤流
     *
     *
     * SQL语义
     * 上面的SQL会翻译成以下三个算子
     * 1.数据源:数据源即最新的词条下面的搜索词的搜索热度数据，消费到Kafka中数据后，按照partition key将数据
     * 进行hash分发到下游排序算子，相同的key数据将会发送到一个并发中
     * 2.排序算子:为每个Key维护了一个TopN的榜单数据，接受到上游的一条数据后，如果TopN榜单还没有到达N
     * 条，则将这条数据加入TopN榜单后，直接下发数据，如果到达N条之后，经过TopN计算，发现这条数据比原有
     * 的数据排序靠前，那么新的TopN排名就会有变化，就变化了的这部分数据之前下发的排名数据撤回（即回撤数
     * 据，然后下发新的排名数据
     * 3.数据汇:接收到上游的数据之后，然后输出到外部存储引擎中
     * 上面三个算子也是会24小时一直运行的。
     */
  }
}
