package flink.sql.udf.typeInference;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;

// 1. 解耦类型推导与 eval ⽅法，类型推导根据 FunctionHint 注解中的信息来，
// 下⾯的案例说明当前这个 UDF 有三种输⼊输出类型信息组合
@FunctionHint(
        input = {@DataTypeHint("INT"),@DataTypeHint("INT")},
        output = @DataTypeHint("INT")
)
@FunctionHint(
        input = {@DataTypeHint("BIGINT"),@DataTypeHint("BIGINT")},
        output = @DataTypeHint("BIGINT")
)
@FunctionHint(
        input = {},
        output = @DataTypeHint("BOOLEAN")
)
public class FunctionHintDemo extends TableFunction<Object> {
    public void eval(Object... o){
        if (o.length == 0){
            collect(false);
        }
        collect(o[0]);
    }
}
