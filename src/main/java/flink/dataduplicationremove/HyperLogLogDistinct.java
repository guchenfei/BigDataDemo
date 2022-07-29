package flink.dataduplicationremove;

import net.agkn.hll.HLL;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 基于 HyperLogLogHyperLogLog 是一种估计统计算法，
 * 被用来统计一个集合中不同数据的个数，也就是我们所说的去重统计。
 * HyperLogLog 算法是用于基数统计的算法，每个 HyperLogLog 键只需要花费 12 KB 内存，就可以计算接近 2 的 64 方个不同元素的基数。
 * HyperLogLog 适用于大数据量的统计，因为成本相对来说是更低的，最多也就占用 12KB 内存。
 * 我们在不需要 100% 精确的业务场景下，可以使用这种方法进行统计。
 * 首先新增依赖：HTML, XML<dependency>  <groupId>net.agkn</groupId>  <artifactId>hll</artifactId>  <version>1.6.0</version></dependency>我们还是以上述的商品 SKU 访问量作为业务场景，数据格式为 <SKU,  访问的用户 id>:
 */
public class HyperLogLogDistinct implements AggregateFunction<Tuple2<String, Long>, HLL, Long> {
    @Override
    public HLL createAccumulator() {
        return new HLL(14, 5);
    }

    @Override
    public HLL add(Tuple2<String, Long> value, HLL accumulator) {
        //value 为访问记录 <商品sku, 用户id>
        accumulator.addRaw(value.f1);
        return accumulator;
    }

    @Override
    public Long getResult(HLL accumulator) {
        long cardinality = accumulator.cardinality();
        return cardinality;
    }

    @Override
    public HLL merge(HLL a, HLL b) {
        a.union(b);
        return a;
    }
}
