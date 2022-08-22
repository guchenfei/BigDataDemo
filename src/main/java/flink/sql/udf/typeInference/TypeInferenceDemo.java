package flink.sql.udf.typeInference;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.Optional;

public class TypeInferenceDemo extends ScalarFunction {
    public Object eval(String s,String type){
        switch (type){
            case "INT":
                return Integer.valueOf(s);
            case "DOUBLE":
                return Double.valueOf(s);
            case "STRING":
            default:
                return s;
        }
    }

    // 如果实现了 getTypeInference，则会禁⽤⾃动的反射式类型推导，使⽤如下逻辑进⾏类型推导
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
        // 指定输⼊参数的类型，必要时参数会被隐式转换
        .typedArguments(DataTypes.STRING(),DataTypes.STRING())
        // ⽤户⾼度⾃定义的类型推导逻辑
        .outputTypeStrategy(callContext -> {
            if (!callContext.isArgumentLiteral(1)||callContext.isArgumentNull(1)){
                throw callContext.newValidationError("Literal expected for second argument.");
            }

            // 基于第二个⼊参决定具体的返回数据类型
            final String literal = callContext.getArgumentValue(1,String.class).orElse("");
            switch (literal){
                case "INT":
                    return Optional.of(DataTypes.INT().notNull());
                case "DOUBLE":
                    return Optional.of(DataTypes.DOUBLE().notNull());
                case "STRING":
                default:
                    return Optional.of(DataTypes.STRING());
            }
        }).build();
    }
}
