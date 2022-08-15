package flink.datatype;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UserDefinedDataTypeDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        String sql = "-- 1. 创建 UDF\n" +
                "CREATE FUNCTION user_scalar_func AS 'flink.examples.sql._12_data_type._02_user_defined.UserScalarFunction';\n" +
                "\n" +
                "-- 2. 创建数据源表\n" +
                "CREATE TABLE source_table (\n" +
                "    user_id BIGINT NOT NULL COMMENT '用户 id'\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.user_id.min' = '1',\n" +
                "  'fields.user_id.max' = '10'\n" +
                ");\n" +
                "\n" +
                "-- 3. 创建数据汇表\n" +
                "CREATE TABLE sink_table (\n" +
                "    result_row_1 ROW<age INT, name STRING, totalBalance DECIMAL(10, 2)>,\n" +
                "    result_row_2 STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");\n" +
                "\n" +
                "-- 4. SQL 查询语句\n" +
                "INSERT INTO sink_table\n" +
                "select\n" +
                "    -- 4.a. 用户自定义类型作为输出\n" +
                "    user_scalar_func(user_id) as result_row_1,\n" +
                "    -- 4.b. 用户自定义类型作为输出及输入\n" +
                "    user_scalar_func(user_scalar_func(user_id)) as result_row_2\n" +
                "from source_table;";

        // "-- 5. 查询结果\n" +
        // "+I[+I[9, name2, 2.20], name2]\n" +
        // "+I[+I[1, name1, 1.10], name1]\n" +
        // "+I[+I[5, name1, 1.10], name1]";
    }
}
