package flink.sql.module;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.module.hive.HiveModule;

public class FlinkUseHiveUDF {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        String name = "default";
        String version = "3.1.2";
        tableEnv.loadModule(name,new HiveModule(version));
        String[] modules = tableEnv.listModules();
        for (String module : modules) {
            System.out.println(module);
        }

        /**
         * 结果:
         * core
         * default
         */

        String[] functions = tableEnv.listFunctions();
        for (String function : functions) {
            System.out.println(function);
        }
    }
}
