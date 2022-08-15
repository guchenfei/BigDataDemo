package flink.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SqlEventTimeDemo {
    public static void main(String[] args) throws Exception {
        String sql = "CREATE TABLE user_actions (\n" +
                "  user_name STRING,\n" +
                "  data STRING,\n" +
                "  user_action_time TIMESTAMP(3),\n" +
                "  -- 使用下面这句来将 user_action_time 声明为事件时间，并且声明 watermark 的生成规则，即 user_action_time 减 5 秒\n" +
                "  -- 事件时间列的字段类型必须是 TIMESTAMP 或者 TIMESTAMP_LTZ 类型\n" +
                "  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  ...\n" +
                ");\n" +
                "\n" +
                "SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)\n" +
                "FROM user_actions\n" +
                "-- 然后就可以在窗口算子中使用 user_action_time\n" +
                "GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);";

        //如果时间戳不是timeStamp,通常是bigInt转化方案如下
        String sql1 = "CREATE TABLE user_actions (\n" +
                "  user_name STRING,\n" +
                "  data STRING,\n" +
                "  -- 1. 这个 ts 就是常见的毫秒级别时间戳\n" +
                "  ts BIGINT,\n" +
                "  -- 2. 将毫秒时间戳转换成 TIMESTAMP_LTZ 类型\n" +
                "  time_ltz AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "  -- 3. 使用下面这句来将 user_action_time 声明为事件时间，并且声明 watermark 的生成规则，即 user_action_time 减 5 秒\n" +
                "  -- 事件时间列的字段类型必须是 TIMESTAMP 或者 TIMESTAMP_LTZ 类型\n" +
                "  WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  ...\n" +
                ");\n" +
                "\n" +
                "SELECT TUMBLE_START(time_ltz, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)\n" +
                "FROM user_actions\n" +
                "GROUP BY TUMBLE(time_ltz, INTERVAL '10' MINUTE);";

        //还有一种指定事件时间戳方式,先DataStream指定然后sql使用
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 1. 分配 watermark
        DataStream<Row> r = env.addSource(new UserDefinedSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.minutes(0L)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        return (long) element.getField(Integer.parseInt("f2"));
                    }
                });
        // 2. 使用 f2.rowtime 的方式将 f2 字段指为事件时间时间戳
        Table sourceTable = tEnv.fromDataStream(r, "f0, f1, f2.rowtime");

        tEnv.createTemporaryView("source_table", sourceTable);

        // 3. 在 tumble window 中使用 f2
        String tumbleWindowSql =
                "SELECT TUMBLE_START(f2, INTERVAL '5' SECOND), COUNT(DISTINCT f0)\n"
                        + "FROM source_table\n"
                        + "GROUP BY TUMBLE(f2, INTERVAL '5' SECOND)";
        Table resultTable = tEnv.sqlQuery(tumbleWindowSql);
        tEnv.toAppendStream(resultTable, Row.class).print();
        env.execute("");
    }

    private static class UserDefinedSource implements SourceFunction<Row>, ResultTypeQueryable<Row> {

        private volatile boolean isCancel;

        @Override
        public void run(SourceContext<Row> sourceContext) throws Exception {

            int i = 0;

            while (!this.isCancel) {

                sourceContext.collect(Row.of("a" + i, "b", System.currentTimeMillis()));

                Thread.sleep(10L);
                i++;
            }

        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            return new RowTypeInfo(TypeInformation.of(String.class), TypeInformation.of(String.class),
                    TypeInformation.of(Long.class));
        }
    }
}
