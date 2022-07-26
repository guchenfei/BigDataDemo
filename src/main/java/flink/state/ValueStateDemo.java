package flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;

import static org.apache.flink.runtime.state.memory.MemoryStateBackend.DEFAULT_MAX_STATE_SIZE;

public class ValueStateDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 5L), Tuple2.of(1L, 2L))
                .keyBy(0)
                .flatMap(new CountWindowAverage())
                .printToErr();
        // false 代表关闭异步快照机制
        //MemoryStateBackend JobManager内存
        env.setStateBackend(new MemoryStateBackend(DEFAULT_MAX_STATE_SIZE,false));

        //FsStateBackend TaskManager的内存 少量的元数据信息存储到 JobManager 的内存中
        // CheckPoint时，将状态快照写入到配置的文件系统目录中
        //env.setStateBackend(new FsStateBackend("hdfs://namenode:40010/flink/checkpoints", false));

        // RocksDBStateBackend 正在运行中的状态数据保存在 RocksDB 数据库中
        //RocksDBStateBackend 可以存储远超过 FsStateBackend 的状态，可以避免向 FsStateBackend 那样一旦出现状态暴增会导致 OOM
        //直接放到磁盘必然导致吞吐量下降
        //env.setStateBackend(new RocksDBStateBackend(),);
        env.execute("submit job");
    }

    public static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
        private transient ValueState<Tuple2<Long, Long>> sum;

        @Override
        public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
            Tuple2<Long, Long> currentSum;
            // 访问ValueState
            if (sum.value() == null) {
                currentSum = Tuple2.of(0L, 0L);
            } else {
                currentSum = sum.value();
            }
            // 更新
            currentSum.f0 += 1;
            // 第二个元素加1
            currentSum.f1 += input.f1;
            // 更新state
            sum.update(currentSum);
            // 如果count的值大于等于2，求知道并清空state
            if (currentSum.f0 >= 2) {
                out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
                sum.clear();
            }
        }

        public void open(Configuration config) {
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>("average",
                    // state的名字
                    TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                    }));
            // 设置默认值
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(10))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            descriptor.enableTimeToLive(ttlConfig);
            sum = getRuntimeContext().getState(descriptor);
        }
    }
}
