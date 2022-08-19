package flink.sql.timezone;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 关于时间时间的时区问题有两种
 * 1.时间戳是不带时区(TIMESTAMP(3)):这类不受时区变动影响
 * 2.时间戳带时区(TIMESTAMP_LTZ):这类会受时区变动的影响
 */
public class EventTimeDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment, settings);

        //EventTime TIMESTAMP(3)
        String sql = "CREATE TABLE MyTable2 (\n" +
                " item STRING,\n" +
                " price DOUBLE,\n" +
                " ts TIMESTAMP(3),\n" +
                " watermark for ts as ts - INTERVAL '10' SECOND \n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'test1',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'csv',\n" +
                " 'scan.startup.mode' = 'latest-offset'\n" +
                ")";

        tableEnvironment.sqlUpdate(sql);

        String sql1 = "create table sink_table(window_start TIMESTAMP(3),window_end TIMESTAMP(3),window_rowtime TIMESTAMP(3),item String,max_price Double)" +
                "with('connector' = 'print')";

        tableEnvironment.sqlUpdate(sql1);

        String sql2 = "insert into sink_table" +
                "      SELECT" +
                "            TUMBLE_START(ts, INTERVAL '10' MINUTES) AS window_start," +
                "            TUMBLE_END(ts, INTERVAL '10' MINUTES) AS window_end," +
                "            TUMBLE_ROWTIME(ts, INTERVAL '10' MINUTES) as window_rowtime," +
                "            item," +
                "            MAX(price) as max_price" +
                "      FROM MyTable2" +
                "      GROUP BY TUMBLE(ts, INTERVAL '10' MINUTES), item";


        /**
         * 我改动了通过print方式打印,示例结果为view
         * Flink SQL> SET table.local-time-zone=UTC;
         * Flink SQL> SELECT * FROM MyView4;
         *
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         * |            window_start |              window_end |          window_rowtime | item | max_price |
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    A |       1.8 |
         * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    B |       2.5 |
         * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    C |       3.8 |
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         *
         * Flink SQL> SET table.local-time-zone=Asia/Shanghai;
         * Flink SQL> SELECT * FROM MyView4;
         *
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         * |            window_start |              window_end |          window_rowtime | item | max_price |
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    A |       1.8 |
         * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    B |       2.5 |
         * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    C |       3.8 |
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         */
        tableEnvironment.executeSql(sql2);
//        tableEnvironment.toAppendStream(tableEnvironment.sqlQuery(sql2), Row.class).print();
        //executionEnvironment.execute();//前边执行了executeSql,该操作会报错
        /**
         * Exception in thread "main" java.lang.IllegalStateException: No operators defined in streaming topology. Cannot execute.
         * 	at org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getStreamGraphGenerator(StreamExecutionEnvironment.java:1872)
         * 	at org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getStreamGraph(StreamExecutionEnvironment.java:1863)
         * 	at org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getStreamGraph(StreamExecutionEnvironment.java:1848)
         * 	at org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.execute(StreamExecutionEnvironment.java:1699)
         * 	at org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.execute(StreamExecutionEnvironment.java:1681)
         * 	at flink.sql.timezone.TimeStampDemo.main(TimeStampDemo.java:48)
         */

        //EventTime TIMESTAMP_LTZ
        String sq3 = "CREATE TABLE MyTable3 (\n" +
                "                  item STRING,\n" +
                "                  price DOUBLE,\n" +
                "                  ts BIGINT, -- long 类型的时间戳\n" +
                "                  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3), -- 转为 TIMESTAMP_LTZ 类型的时间戳\n" +
                "                  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '10' SECOND\n" +
                "            ) WITH (\n" +
                "                'connector' = 'socket',\n" +
                "                'hostname' = '127.0.0.1',\n" +
                "                'port' = '9999',\n" +
                "                'format' = 'csv'\n" +
                "           );" +
                "CREATE VIEW MyView5 AS \n" +
                "            SELECT \n" +
                "                TUMBLE_START(ts_ltz, INTERVAL '10' MINUTES) AS window_start,        \n" +
                "                TUMBLE_END(ts_ltz, INTERVAL '10' MINUTES) AS window_end,\n" +
                "                TUMBLE_ROWTIME(ts_ltz, INTERVAL '10' MINUTES) as window_rowtime,\n" +
                "                item,\n" +
                "                MAX(price) as max_price\n" +
                "            FROM MyTable3\n" +
                "                GROUP BY TUMBLE(ts_ltz, INTERVAL '10' MINUTES), item;";

        /**
         * DESC MyView5;
         *
         * +----------------+----------------------------+-------+-----+--------+-----------+
         * |           name |                       type |  null | key | extras | watermark |
         * +----------------+----------------------------+-------+-----+--------+-----------+
         * |   window_start |               TIMESTAMP(3) | false |     |        |           |
         * |     window_end |               TIMESTAMP(3) | false |     |        |           |
         * | window_rowtime | TIMESTAMP_LTZ(3) *ROWTIME* |  true |     |        |           |
         * |           item |                     STRING |  true |     |        |           |
         * |      max_price |                     DOUBLE |  true |     |        |           |
         * +----------------+----------------------------+-------+-----+--------+-----------+
         */

        //执行上边逻辑对应结果为
        /**
         * Flink SQL> SET table.local-time-zone=UTC;
         * Flink SQL> SELECT * FROM MyView5;
         *
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         * |            window_start |              window_end |          window_rowtime | item | max_price |
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    A |       1.8 |
         * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    B |       2.5 |
         * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    C |       3.8 |
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         *
         * Flink SQL> SET table.local-time-zone=Asia/Shanghai;
         * Flink SQL> SELECT * FROM MyView5;
         *
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         * |            window_start |              window_end |          window_rowtime | item | max_price |
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         * | 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    A |       1.8 |
         * | 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    B |       2.5 |
         * | 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    C |       3.8 |
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         */
    }
}
