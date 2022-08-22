package flink.sql.udf.typeInference;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

// 2. 为函数类的所有 eval ⽅法指定同⼀个输出类型
@FunctionHint(output = @DataTypeHint("ROW<s STRING,i INT>"))
public class FunctionHintDemo2 extends TableFunction<Row> {
    public void eval(int a,int b){
        collect(Row.of("Sum",a+b));
    }

    public void eval(){
        collect(Row.of("Empty args",-1));
    }
}
