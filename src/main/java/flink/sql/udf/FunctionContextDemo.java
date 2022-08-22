package flink.sql.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class FunctionContextDemo extends ScalarFunction {
    private int factor = 0;

    @Override
    public void open(FunctionContext context) throws Exception {
        // 4. 在 UDF 中获取全局参数 hashcode_factor
        // ⽤户可以配置全局作业参数 "hashcode_factor"
        // 获取参数 "hashcode_factor"
        // 如果不存在，则使⽤默认值 "12"
        factor = Integer.parseInt(context.getJobParameter("hashcode_factor", "12"));
    }

    public int eval(String s) {
        return s.hashCode() * factor;
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment, settings);
        // 1. 设置任务参数
        tableEnv.getConfig().addJobParameter("hashcode_factor", "31");
        // 2. 注册函数
        tableEnv.createTemporarySystemFunction("hashCode", FunctionContextDemo.class);
        // 3. 调⽤函数
        tableEnv.sqlQuery("select myField,hashCode(myField) from myTable");
    }
}
