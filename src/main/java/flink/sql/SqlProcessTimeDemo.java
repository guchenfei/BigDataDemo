package flink.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SqlProcessTimeDemo {
    public static void main(String[] args) throws Exception {
        String sql = "CREATE TABLE user_actions (\n" +
                "  user_name STRING,\n" +
                "  data STRING,\n" +
                "  -- 使用下面这句来将 user_action_time 声明为处理时间\n" +
                "  user_action_time AS PROCTIME()\n" +
                ") WITH (\n" +
                "  ...\n" +
                ");\n" +
                "\n" +
                "SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)\n" +
                "FROM user_actions\n" +
                "-- 然后就可以在窗口算子中使用 user_action_time\n" +
                "GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);";

        //方式二:DataStream指定处理时间
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<Row> r = env.addSource(new UserDefinedSource());
        // 2. 使用 proctime.proctime 方式添加处理时间的时间戳
        Table sourceTable = tEnv.fromDataStream(r, "f0, f1, f2, proctime.proctime");
        tEnv.createTemporaryView("source_table", sourceTable);

        // 3. 在tumble window中使用proctime,按照处理时间来执行滚动窗口
        String tumbleWindowSql =
                "SELECT TUMBLE_START(proctime, INTERVAL '5' SECOND), COUNT(DISTINCT f0)\n"
                        + "FROM source_table\n"
                        + "GROUP BY TUMBLE(proctime, INTERVAL '5' SECOND)";

        Table resultTable = tEnv.sqlQuery(tumbleWindowSql);
        tEnv.toAppendStream(resultTable, Row.class).print();
        env.execute();
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


