package flink.sql.udtf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 注意如果使用scala实现UDF,不要使用object类,单例模式会影响到并发
 */
public class SplitFunction extends TableFunction<Row> {
    public void eval(String str) {
        for (String s : str.split(" ")) {
            //输出结果
            collect(Row.of(s, s.length()));
        }
    }

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        //在Table api 里直接调用UDF
        tableEnv.from("MyTable")
                .joinLateral(call(SplitFunction.class, $("myField")))
                .select($("myField"), $("word"), $("length"));

        tableEnv.from("MyTable")
                .leftOuterJoinLateral(call(SplitFunction.class, $("myField")))
                .select($("myField"),$("word"),$("length"));


        //在Table Api中重命名UDF结果字段
        tableEnv.from("MyTable")
                .leftOuterJoinLateral(call(SplitFunction.class,$("myField")).as("newWord","newLength"))
                .select($("myField"),$("newWord"),$("newLength"));

        tableEnv.createTemporarySystemFunction("SplitFunction",SplitFunction.class);
        //在Table Api里调用注册好的UDF
        tableEnv.from("MyTable")
                .joinLateral(call("SplitFunction",$("myField")))
                .select($("myField"),$("word"),$("length"));

        tableEnv.from("MyTable")
                .leftOuterJoinLateral(call("SplitFunction",$("myField")))
                .select($("myField"),$("word"),$("length"));


        //在Sql里边调用注册好的UDF
        tableEnv.sqlQuery("select myField,word,length " +
                "from MyTable" +
                "left join lateral table(SplitFunction(myField)) on true");

        //在Sql中重命名UDF字段
        tableEnv.sqlQuery("select myField,newWord,newLength" +
                "from MyTable" +
                "left join lateral table(SplitFunction(myField)) as T(newWord,newLength) on true");

    }
}
