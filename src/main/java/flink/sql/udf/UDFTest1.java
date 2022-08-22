package flink.sql.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class UDFTest1 {
    /**
     * 举例在 ScalarFunction 中：
     * 1. ⭐open()：⽤于初始化资源（⽐如连接外部资源），程序初始化时进⾏调⽤
     * 2. ⭐close()：⽤于关闭资源，程序结束时进⾏调⽤
     * 3. ⭐isDeterministic()：⽤于判断返回结果是否是确定的，如果是确定的，结果会被直接执⾏
     * 4. ⭐eval(xxx)：Flink ⽤于处理每⼀条数据的主要处理逻辑函数
     * 你可以⾃定义 eval 的⼊参，⽐如：
     * eval(Integer) 和 eval(LocalDateTime)；
     * 使⽤变⻓参数，例如 eval(Integer…);
     * 使⽤对象，例如 eval(Object) 可接受 LocalDateTime、Integer 作为参数，只要是 Object 都可以；
     * 也可组合使⽤，例如 eval(Object…) 可接受所有类型的参数。
     */
    static class SubStringFunction extends ScalarFunction {
        private boolean endInclusive;

        public SubStringFunction(boolean endInclusive) {
            this.endInclusive = endInclusive;
        }

        public String eval(String s, Integer begin, Integer end) {
            return s.substring(begin, endInclusive ? end + 1 : end);
        }
    }

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        //Table Api 调用UDF
        tableEnv.from("MyTable").select(call(new SubStringFunction(true), $("myField"), 5, 12));
        //注册UDF
        tableEnv.createTemporarySystemFunction("SubStringFunction", new SubStringFunction(true));
    }
}
