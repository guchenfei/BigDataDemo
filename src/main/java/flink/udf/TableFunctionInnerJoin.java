package flink.udf;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import java.util.Arrays;

/**
 * 自定义列转行
 */
public class TableFunctionInnerJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定是EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, bsSettings);

        String sql = "CREATE FUNCTION user_profile_table_func AS 'com.yyds.flink_sql.ddl_create"
                + "._07_TableFunctionInnerJoin$UserProfileTableFunction';\n"
                + "\n"
                + "CREATE TABLE source_table (\n"
                + "    user_id BIGINT NOT NULL,\n"
                + "    name STRING,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
                + "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '10',\n"
                + "  'fields.name.length' = '1',\n"
                + "  'fields.user_id.min' = '1',\n"
                + "  'fields.user_id.max' = '10'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE sink_table (\n"
                + "    user_id BIGINT,\n"
                + "    name STRING,\n"
                + "    age INT,\n"
                + "    row_time TIMESTAMP(3)\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "\n"
                + "INSERT INTO sink_table\n"
                + "SELECT user_id,\n"
                + "       name,\n"
                + "       age,\n"
                + "       row_time\n"
                + "FROM source_table,\n"
                // Table Function Join 语法对应 LATERAL TABLE
                + "LATERAL TABLE(user_profile_table_func(user_id)) t(age)";

        Arrays.stream(sql.split(";"))
                .forEach(tabEnv::executeSql);
    }

    public static class UserProfileTableFunction extends TableFunction<Integer> {

        public void eval(long userId) {
            // 自定义输出逻辑
            if (userId <= 5) {
                // 一行转 1 行
                collect(1);
            } else {
                // 一行转 3 行
                collect(1);
                collect(2);
                collect(3);
            }
        }

    }
}

