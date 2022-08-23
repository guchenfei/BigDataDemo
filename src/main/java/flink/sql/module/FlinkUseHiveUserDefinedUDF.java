package flink.sql.module;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkUseHiveUserDefinedUDF {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment env = StreamTableEnvironment.create(streamEnv, settings);

        String sql = "create table sink_table(" +
                "user_id BIGINT," +
                "log_id STRING" +
                ")WITH(" +
                "'connector'='print');" +
                "" +
                "insert into sink_table" +
                "select user_id,test_hive_udf(params) as log_id";

        //怎么注册到hive Module中需要研究
//       flinkEnv.hiveModuleV2()
//               .registryHiveUDF("test_hive_udf",HiveUDF.class.getName());
    }
}
