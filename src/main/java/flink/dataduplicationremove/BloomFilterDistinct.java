package flink.dataduplicationremove;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.shaded.curator4.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.curator4.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 基于布隆过滤器（BloomFilter）
 * BloomFilter（布隆过滤器）类似于一个 HashSet，用于快速判断某个元素是否存在于集合中，其典型的应用场景就是能够快速判断一个 key 是否存在于某容器，不存在就直接返回。
 * 需要注意的是，和 HyperLogLog 一样，布隆过滤器不能保证 100% 精确。
 * 但是它的插入和查询效率都很高。我们可以在非精确统计的情况下使用这种方法：
 */
public class BloomFilterDistinct extends KeyedProcessFunction<Long, String, Long> {
    private transient ValueState<BloomFilter> bloomState;
    private transient ValueState<Long> countState;

    @Override
    public void processElement(String value, Context ctx, Collector<Long> out) throws Exception {
        BloomFilter bloomFilter = bloomState.value();
        Long skuCount = countState.value();
        if (bloomFilter == null) {
            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 10000000);
        }
        if (skuCount == null) {
            skuCount = 0L;
        }
        if (!bloomFilter.mightContain(value)) {
            bloomFilter.put(value);
            skuCount = skuCount + 1;
        }
        bloomState.update(bloomFilter);
        countState.update(skuCount);
        out.collect(countState.value());
    }
}
