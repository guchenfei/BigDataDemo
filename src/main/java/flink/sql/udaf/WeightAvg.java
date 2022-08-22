package flink.sql.udaf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionRequirement;
import org.elasticsearch.search.aggregations.metrics.WeightedAvg;

import java.util.Set;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 聚合函数即 UDAF，常⽤于进多条数据，出⼀条数据的场景。
 * <p>
 * 使⽤ Java\Scala 开发⼀个 Aggregate Function 必须包含以下⼏点：
 * 1. ⭐实现 AggregateFunction 接⼝，其中所有的⽅法必须是 public 的、⾮ static 的
 * 2. ⭐必须实现以下⼏个⽅法：
 * ⭐Acc聚合中间结果 createAccumulator()：为当前 Key 初始化⼀个空的 accumulator，其存储了聚合的中间结果，
 * ⽐如在执⾏ max() 时会存储当前的 max 值
 * ⭐accumulate(Acc accumulator, Input输⼊参数)：对于每⼀⾏数据，都会调⽤ accumulate() ⽅法来更新
 * accumulator，这个⽅法就是⽤于处理每⼀条输⼊数据；这个⽅法必须声明为 public 和⾮ static 的。accumulate ⽅
 * 法可以重载，每个⽅法的参数类型可以不同，并且⽀持变⻓参数。
 * ⭐Output输出参数 getValue(Acc accumulator)：通过调⽤ getValue ⽅法来计算和返回最终的结果
 * 3. ⭐还有⼏个⽅法是在某些场景下才必须实现的：
 * ⭐retract(Acc accumulator, Input输⼊参数)：在回撤流的场景下必须要实现，Flink 在计算回撤数据时需要进⾏调
 * ⽤，如果没有实现则会直接报错
 * ⭐merge(Acc accumulator, Iterable<Acc> it)：在许多批式聚合以及流式聚合中的 Session、Hop 窗⼝聚合场景下都
 * 是必须要实现的。除此之外，这个⽅法对于优化也很多帮助。例如，如果你打开了两阶段聚合优化，就需要
 * AggregateFunction 实现 merge ⽅法，从⽽可以做到在数据进⾏ shuffle 前先进⾏⼀次聚合计算。
 * ⭐resetAccumulator()：在批式聚合中是必须实现的。
 * 4. ⭐还有⼏个关于⼊参、出参数据类型信息的⽅法，默认情况下，⽤户的 Input输⼊参数（accumulate(Acc
 * accumulator, Input输⼊参数) 的⼊参 Input输⼊参数）、accumulator（Acc聚合中间结果 createAccumulator() 的返回结
 * 果）、Output输出参数 数据类型（Output输出参数 getValue(Acc accumulator) 的 Output输出参数）都会被 Flink 使⽤
 * 反射获取到。但是对于 accumulator 和 Output输出参数 类型来说，Flink SQL 的类型推导在遇到复杂类型的时候可能
 * 会推导出错误的结果（注意：Input输⼊参数 因为是上游算⼦传⼊的，所以类型信息是确认的，不会出现推导错误的情
 * 况），⽐如那些⾮基本类型 POJO 的复杂类型。所以跟 ScalarFunction 和 TableFunction ⼀样，
 * AggregateFunction 提供了 AggregateFunction#getResultType() 和 AggregateFunction#getAccumulatorType() 来分
 * 别指定最终返回值类型和 accumulator 的类型，两个函数的返回值类型都是 TypeInformation，所以熟悉
 * DataStream 的⼩伙伴很容易上⼿。
 * ⭐getResultType()：即 Output输出参数 getValue(Acc accumulator) 的输出结果数据类型
 * ⭐getAccumulatorType()：即 Acc聚合中间结果 createAccumulator() 的返回结果数据类型
 * 这个时候，我们直接来举⼀个加权平均值的例⼦看下，总共 3 个步骤：
 * ⭐定义⼀个聚合函数来计算某⼀列的加权平均
 * ⭐ 在 TableEnvironment 中注册函数
 * ⭐在查询中使⽤函数
 * 为了计算加权平均值，accumulator 需要存储加权总和以及数据的条数。在我们的例⼦⾥，我们定义了⼀个类
 * WeightedAvgAccumulator 来作为 accumulator。
 * Flink 的 checkpoint 机制会⾃动保存 accumulator，在失败时进⾏恢复，以此来保证精确⼀次的语义。
 * 我们的 WeightedAvg（聚合函数）的 accumulate ⽅法有三个输⼊参数。第⼀个是 WeightedAvgAccum accumulator，
 * 另外两个是⽤户⾃定义的输⼊：输⼊的值 ivalue 和 输⼊的权重 iweight。
 * 尽管 retract()、merge()、resetAccumulator() 这⼏个⽅法在⼤多数聚合类型中都不是必须实现的，博主也在样例中提供
 * 了他们的实现。并且定义了 getResultType() 和 getAccumulatorType()。
 */
public class WeightAvg extends AggregateFunction<Long, WeightAvg.WeightedAvgAccumulator> {
    // 创建一个 accumulator
    @Override
    public WeightedAvgAccumulator createAccumulator() {
        return new WeightedAvgAccumulator();
    }

    @Override
    public boolean isDeterministic() {
        return super.isDeterministic();
    }

    // 获取返回结果
    @Override
    public Long getValue(WeightedAvgAccumulator acc) {
        if (acc.count == 0) {
            return null;
        } else {
            return acc.sum / acc.count;
        }
    }

    public void accumulate(WeightedAvgAccumulator acc, Long iValue, Integer iWeight) {
        acc.sum += iValue * iWeight;
        acc.count += iWeight;
    }

    // Session window 可以使用这个方法将几个单独窗口的结果合并
    public void merge(WeightedAvgAccumulator acc, Iterable<WeightedAvgAccumulator> it) {
        for (WeightedAvgAccumulator a : it) {
            acc.count += a.count;
            acc.sum += a.sum;
        }
    }

    public void resetAccumulator(WeightedAvgAccumulator acc) {
        acc.count = 0;
        acc.sum = 0L;
    }


    class WeightedAvgAccumulator {
        public long sum = 0;
        public int count = 0;
    }

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment env = StreamTableEnvironment.create(streamEnv, settings);

        env.from("MyTable")
                .groupBy($("myField"))
                .select($("myField"), call(WeightAvg.class, $("value"), $("weight")));

        // 注册函数
        env.createTemporarySystemFunction("WeightedAvg", WeightAvg.class);

        // Table API 调用函数
        env.from("MyTable")
                .groupBy($("myField"))
                .select($("myField"), call("WeightedAvg", $("value"), $("weight")));

        // SQL API 调用函数
        env.sqlQuery(
                "SELECT myField, WeightedAvg(`value`, `weight`) FROM MyTable GROUP BY myField"
        );
    }
}
