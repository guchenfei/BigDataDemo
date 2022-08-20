package flink.sql.join

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * LookUpJoin（维表Join)
 * LookUpJoin定义（支持Batch/Streaming)
 * LookUpJoin其实就是维表Join,比如拿离线数仓来说，常常会
 * 有用户画像，设备画像等数据，而对应到实时数仓场景中，这种实时获取外部缓存的Join就叫做维表Join
 * 应用场景、小伙伴会问，我们既然己经有了上面介绍的RegularJoin，IntervalJoin等，为啥还需要一种
 * LookUpJoin?因为上面说的这几种Join都是流与流之间的Join,而LookUpJoin是流与Redis,Mysql，HBase
 * 这种存储介质的Join。Lookup的意思就是实时查找，而实时的画像数据一般都是存储在Redis,Mysql,HBase
 * 中，这就是LookJoin的由来
 * 实际案例：曝光用户日志流(show_log关联用户画像维表(user_profile)关联到用户的维度之后，提供
 * 给下游计算分性别，年龄段的曝光用户数使用。
 */
object LookupJoin {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    /**
     * 曝光用户日志流,来自Kafka
     * log_id	    timestamp	       user_id
     * 1       2021-11-01 00:01:03	a
     * 2       2021-11-01 00:03:00	b
     * 3       2021-11-01 00:05:00	c
     * 4       2021-11-01 00:06:00	b
     * 5       2021-11-01 00:07:00	c
     *
     * 用户画像维表(user_profile) 来自redis
     * user_id(主键)	    age	    sex
     * a               12-18    男
     * b               18-24    女
     * c               18-24    男
     *
     * redis中的数据结构<key,value> key为user_id,value age,sex的json
     */

    val sql = "CREATE TABLE show_log (\n" +
      "log_id BIGINT,\n" +
      "`timestamp` as cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
      "user_id STRING,\n" +
      "proctime AS PROCTIME())\n" +
      "WITH (" +
      "'connector' = 'datagen'," +
      "'rows-per-second' = '10'," +
      "'fields.user_id.length' = '1'," +
      "'fields.log_id.min' = '1'," +
      "'fields.log_id.max' = '10')"

    val sql2 = "CREATE TABLE user_profile (\n" +
      "user_id STRING,\n" +
      "age STRING,\n" +
      "sex STRING)\n " +
      "WITH (" +
      "'connector' = 'redis'," +
      "'hostname' = '127.0.0.1'," +
      "'port' = '6379'," +
      "'format' = 'json'," +
      "'lookup.cache.max-rows' = '500'," +
      "'lookup.cache.ttl' = '3600'," +
      "'lookup.max-retries' = '1')"

    val sql3 = "CREATE TABLE sink_table (\n" +
      "log_id BIGINT,\n" +
      "`timestamp` TIMESTAMP(3),\n" +
      "user_id STRING,\n" +
      "proctime TIMESTAMP(3),\n" +
      "age STRING,\n" +
      "sex STRING)\n " +
      "WITH ('connector' = 'print')"

    val sql4 = "INSERT INTO sink_table\n" +
      "SELECT s.log_id as log_id,\n" +
      "s.`timestamp` as `timestamp`,\n" +
      "s.user_id as user_id,\n" +
      "s.proctime as proctime,\n" +
      "u.sex as sex, \n " +
      "u.age as age\n" +
      "FROM show_log AS s \n" +
      "LEFT JOIN user_profile FOR SYSTEM_TIME AS OF s.proctime AS u ON s.user_id = u.user_id"

    /**
     * 实时的LookUp维表关联可以通过processTime来关联
     * 此刻redis的connector是自定义的
     * 结果示例
     * log_id  timestamp               user_id age	    sex
     * 1       2021-11-01 00:01:03 a       12-18         男
     * 2       2021-11-01 00:03:00 b       18-24         女
     * 3       2021-11-01 00:05:00 c       18-24         男
     * 4       2021-11-01 00:06:00 b       18-24         女
     * 5       2021-11-01 00:07:00 c       18-24         男
     */
    tableEnv.executeSql(sql)
    tableEnv.executeSql(sql2)
    tableEnv.executeSql(sql3)
    tableEnv.executeSql(sql4)
  }
}
