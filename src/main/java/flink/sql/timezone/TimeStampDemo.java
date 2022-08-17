package flink.sql.timezone;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TimeStampDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment, settings);

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
    }
}
