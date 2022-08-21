package flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableResult}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 应用场景:Explain子句用于查看当前sql查询的逻辑计划和优化后的执行计划
 * EXPLAIN PLAN FOR <query_statement_or_insert_statement>
 */
object Explain {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //案例:
    val sql4 = "CREATE TABLE source_table (\n" +
      "user_id BIGINT COMMENT '用户 id',\n" +
      "name STRING COMMENT '用户姓名',\n" +
      "server_timestamp BIGINT COMMENT '用户访问时间戳',\n" +
      "proctime AS PROCTIME()\n) " +
      "WITH ('connector' = 'datagen'," +
      "'rows-per-second' = '1'," +
      "'fields.name.length' = '1'," +
      "'fields.user_id.min' = '1'," +
      "'fields.user_id.max' = '10'," +
      "'fields.server_timestamp.min' = '1'," +
      "'fields.server_timestamp.max' = '100000')"

    //-- 数据汇：根据 user_id 去重的第一条数据
    val sql5 = "CREATE TABLE sink_table (\n" +
      "user_id BIGINT,\n" +
      "name STRING,\n" +
      "server_timestamp BIGINT\n) " +
      "WITH ('connector' = 'print')"

    //-- 处理逻辑：
    val sql6 = "INSERT INTO sink_table\n" +
      "select user_id," +
      "name," +
      "server_timestamp\n" +
      "from (SELECT user_id,name,server_timestamp," +
      "        row_number() over(partition by user_id order by proctime ASC) as rn\n" +
      "        FROM source_table)\n" +
      "where rn = 1"

    tableEnv.explainSql(sql4)
    tableEnv.explainSql(sql5)
    val result: TableResult = tableEnv.executeSql(sql6)
    result.print()

    /**
     * 逻辑计划结果
     * == Abstract Syntax Tree ==
LogicalSink(table=[default_catalog.default_database.sink_table], fields=[user_id, name, server_timestamp])
+- LogicalProject(user_id=[$0], name=[$1], server_timestamp=[$2])
   +- LogicalFilter(condition=[=($3, 1)])
      +- LogicalProject(user_id=[$0], name=[$1], server_timestamp=[$2], rn=[ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY PROCTIME() NULLS FIRST)])
         +- LogicalTableScan(table=[[default_catalog, default_database, source_table]])

== Optimized Physical Plan ==
Sink(table=[default_catalog.default_database.sink_table], fields=[user_id, name, server_timestamp])
+- Calc(select=[user_id, name, server_timestamp])
   +- Deduplicate(keep=[FirstRow], key=[user_id], order=[PROCTIME])
      +- Exchange(distribution=[hash[user_id]])
         +- Calc(select=[user_id, name, server_timestamp, PROCTIME() AS $3])
            +- TableSourceScan(table=[[default_catalog, default_database, source_table]], fields=[user_id, name, server_timestamp])

== Optimized Execution Plan ==
Sink(table=[default_catalog.default_database.sink_table], fields=[user_id, name, server_timestamp])
+- Calc(select=[user_id, name, server_timestamp])
   +- Deduplicate(keep=[FirstRow], key=[user_id], order=[PROCTIME])
      +- Exchange(distribution=[hash[user_id]])
         +- Calc(select=[user_id, name, server_timestamp, PROCTIME() AS $3])
            +- TableSourceScan(table=[[default_catalog, default_database, source_table]], fields=[user_id, name, server_timestamp])
     */
  }
}
