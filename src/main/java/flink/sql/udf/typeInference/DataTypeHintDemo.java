package flink.sql.udf.typeInference;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;

public class DataTypeHintDemo extends ScalarFunction {
    // 不需要任何声明，可以直接推导出类型信息，即⼊参和出参对应到 SQL 中的 bigint 类型
    public long eval(long a, long b) {
        return a + b;
    }

    // 使⽤ @DataTypeHint("DECIMAL(12, 3)") 定义 decimal 的精度和⼩数位
    public @DataTypeHint("DECIMAL(12,3)")
    BigDecimal eval(double a, double b) {
        return BigDecimal.valueOf(a + b);
    }

    // 使⽤注解定义嵌套数据类型,注意定义类型都是sql语法参数类型,如同建表类型格式
    public @DataTypeHint("ROW<s STRING,t TIMESTAMP_LTZ(3)>") Row eval(int i){
        return Row.of(String.valueOf(i), Instant.ofEpochSecond(i));
    }


    public @DataTypeHint(value = "RAW",bridgedTo = ByteBuffer.class) ByteBuffer eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o){
//        return MyUtils.serializeToByteBuffer(o);
        return null;
    }
}
