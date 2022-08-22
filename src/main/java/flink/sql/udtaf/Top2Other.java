package flink.sql.udtaf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;

public class Top2Other extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Other.Top2Accum> {
    /**
     * Accumulator for Top2.
     */
    public class Top2Accum {
        public Integer first;
        public Integer second;
        public Integer oldFirst;
        public Integer oldSecond;
    }

    @Override
    public Top2Accum createAccumulator() {
        Top2Accum acc = new Top2Accum();
        acc.first = Integer.MIN_VALUE;
        acc.second = Integer.MIN_VALUE;
        acc.oldFirst = Integer.MIN_VALUE;
        acc.oldSecond = Integer.MIN_VALUE;
        return acc;
    }

    public void accumulate(Top2Accum acc, Integer v) {
        if (v > acc.first) {
            acc.second = acc.first;
            acc.first = v;
        } else if (v > acc.second) {
            acc.second = v;
        }
    }

    public void emitUpdateWithRetract(Top2Accum acc, RetractableCollector<Tuple2<Integer, Integer>> out) {
        if (!acc.first.equals(acc.oldFirst)) {
            // if there is an update, retract old value then emit new value.
            if (acc.oldFirst != Integer.MIN_VALUE) {
                out.retract(Tuple2.of(acc.oldFirst, 1));
            }
            out.collect(Tuple2.of(acc.first, 1));
            acc.oldFirst = acc.first;
        }

        if (!acc.second.equals(acc.oldSecond)) {
            // if there is an update, retract old value then emit new value.
            if (acc.oldSecond != Integer.MIN_VALUE) {
                out.retract(Tuple2.of(acc.oldSecond, 2));
            }
            out.collect(Tuple2.of(acc.second, 2));
            acc.oldSecond = acc.second;
        }
    }

    public static void main(String[] args) {
        // 注册函数
//        StreamTableEnvironment tEnv = ...
//        tEnv.registerFunction("top2", new Top2Other());
//
//        // 初始化表
//        Table tab = ...;
//
//        // 使用函数
//        tab.groupBy("key")
//                .flatAggregate("top2(a) as (first, second)")
//                .select($("key"), $("first"), $("second"));
    }
}

