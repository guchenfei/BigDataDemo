package flink.sql.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

public class UDFTest2 extends ScalarFunction {
    //接受任意类型输入返回Int
    public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o){
        return o.hashCode();
    }
}
