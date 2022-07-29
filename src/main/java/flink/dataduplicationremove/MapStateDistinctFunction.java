package flink.dataduplicationremove;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 消除重复数据是我们在实际业务中经常遇到的一类问题。
 * 在大数据领域，重复数据的删除有助于减少存储所需要的存储容量。
 * 而且在一些特定的业务场景中，重复数据是不可接受的，
 * 例如，精确统计网站一天的用户数量、在事实表中统计每天发出的快递包裹数量。
 * 在传统的离线计算中，我们可以直接用 SQL 通过 DISTINCT 函数，或者数据量继续增加时会用到类似 MapReduce 的思想。
 * 那么在实时计算中，去重计数是一个增量和长期的过程，并且不同的场景下因为效率和精度问题方案也需要变化。
 * 针对上述问题，我们在这里列出几种常见的 Flink 中实时去重方案：基于状态后端基于 HyperLogLog基于布隆过滤器（BloomFilter）基于 BitMap基于外部数据库下面我们依次讲解上述几种方案的适用场景和实现原理。
 * 基于状态后端我们在第 09 课时“Flink 状态与容错”中曾经讲过 Flink State 状态后端的概念和原理，其中状态后端的种类之一是 RocksDBStateBackend。
 * 它会将正在运行中的状态数据保存在 RocksDB 数据库中，该数据库默认将数据存储在 TaskManager 运行节点的数据目录下。
 * RocksDB 是一个 K-V 数据库，我们可以利用 MapState 进行去重。这里我们模拟一个场景，计算每个商品 SKU 的访问量。
 */
public class MapStateDistinctFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {
    private transient ValueState<Integer> counts;

    @Override
    public void open(Configuration parameters) throws Exception {
        //我们设置 ValueState 的 TTL 的生命周期为24小时，到期自动清除状态
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(org.apache.flink.api.common.time.Time.minutes(24 * 60))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        //设置 ValueState 的默认值
        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<Integer>("skuNum", Integer.class);
        descriptor.enableTimeToLive(ttlConfig);
        counts = getRuntimeContext().getState(descriptor);
        super.open(parameters);
    }

    @Override
    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        String f0 = value.f0;
        //如果不存在则新增
        if (counts.value() == null) {
            counts.update(1);
        } else {
            //如果存在则加1
            counts.update(counts.value() + 1);
        }
        out.collect(Tuple2.of(f0, counts.value()));
    }
}
