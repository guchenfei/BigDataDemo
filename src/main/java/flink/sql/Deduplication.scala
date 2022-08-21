package flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Deduplication
 * 1.Deduplication定义（支持Batch＼Streaming）：Deduplication 其实就是去重，也即上文介绍到的TopN中 row_number=1
 * 的场景，但是这里有一点不一样在于其排序字段一定是时间属性列，不能是其他非时间属性的普通列。在row＿number＝
 * 时，如果排序字段是普通列 planner 会翻译成TopN算子，如果是时间属性列 planner 会翻译成 Deduplication，这两者最终的执行算子是不一样的，Deduplication相比TopN算子专门做了对应的优化，性能会有很大提升。
 * 2.应用场景：比如上游数据发重了，或者计算DAU明细数据等场景，都可以使用Deduplication 语法去做去重。3.
 *
 * SQL 语法标准：
 * SELECT [column_list]
 * FROM (
 * SELECT [column_list],
 * ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
 * ORDER BY time_attr [asc|desc]) AS rownum
 * FROM table_name)
 * WHERE rownum = 1
 */
object Deduplication {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //案例1(事件时间)：
    //腾讯QQ用户等级场景,每个QQ用户都有一个用户等级,需要求出当前用户等级在星星,月亮,太阳的用户数分别多少
    //-- 数据源：当每一个用户的等级初始化及后续变化的时候的数据，即用户等级变化明细数据。
    val sql = "CREATE TABLE source_table (\n" +
      "user_id BIGINT COMMENT '用户 id',\n" +
      "level STRING COMMENT '用户等级',\n" +
      "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)) COMMENT '事件时间戳',\n" +
      "WATERMARK FOR row_time AS row_time\n) " +
      "WITH ('connector' = 'datagen'," +
      "'rows-per-second' = '1'," +
      "'fields.level.length' = '1'," +
      "'fields.user_id.min' = '1'," +
      "'fields.user_id.max' = '1000000')"

    //-- 数据汇：输出即每一个等级的用户数
    val sql2 = "CREATE TABLE sink_table (\n" +
      "level STRING COMMENT '等级',\n" +
      "uv BIGINT COMMENT '当前等级用户数',\n" +
      "row_time timestamp(3) COMMENT '时间戳'\n) " +
      "WITH ('connector' = 'print')"

    //-- 处理逻辑：
    val sql3 = "INSERT INTO sink_table\n" +
      "select level\n, " +
      "count(1) as uv\n, " +
      "max(row_time) as row_time\n" +
      "from (" +
      "      SELECT user_id,level,row_time," +
      "             row_number() over(partition by user_id order by row_time desc) as rn\n" +
      "      FROM source_table)\n" +
      "where rn = 1\n" +
      "group by level"

    /**
     * 示例结果:发现有回撤数据
     * 3> +U[8, 27, 2022-08-21T11:13:06.298]
     * 1> -U[a, 25, 2022-08-21T11:13:01.298]
     * 8> -U[1, 21, 2022-08-21T11:13:04.301]
     * 1> +U[a, 26, 2022-08-21T11:13:06.298]
     * 8> +U[1, 22, 2022-08-21T11:13:06.298]
     * 2> -U[d, 29, 2022-08-21T11:13:05.297]
     * 2> +U[d, 30, 2022-08-21T11:13:06.298]
     *
     * 其对应的SQL 语义如下：
     * 数据源：消费到Kafka中数据后，将数据按照 partition by的key 通过hash分发策略发送到下游去重算子
     * Deduplication 去重算子：接受到上游数据之后，根据order by中的条件判断当前的这条数据和之前数据时间戳大小，以上面案例来说，如果当前数据时间戳大于之前数据时间戳，则撤回之前向下游发的中间结果，然后将最新的结果发向下游（发送策略也为hash，具体的hash 策略为按照 group by中key进行发送），如果当前数据时间戳小于之前数据时间戳，则不做操作。次算子产出的结果就是每一个用户的对应的最新等级信息。
     * Group by聚合算子：接受到上游数据之后，根据Group by聚合粒度对数据进行聚合计算结果（每一个等级的用户数），发往下游数据汇算子
     * 数据汇：接收到上游的数据之后，然后输出到外部存储引擎中
     */


    /**
     * 案例2（处理时间）：最原始的日志是明细数据，需要我们根据用户 id筛选出这个用户当天的第一条数据，发往下游，下游可以据此计算分各种维度的DAU
     */
    //-- 数据源：原始日志明细数据
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


    tableEnv.executeSql(sql4)
    tableEnv.executeSql(sql5)
    tableEnv.executeSql(sql6)
  }
}
