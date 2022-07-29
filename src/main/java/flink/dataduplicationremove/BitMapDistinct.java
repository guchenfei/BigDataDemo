package flink.dataduplicationremove;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

/**
 * 基于 BitMap上面的 HyperLogLog 和 BloomFilter 虽然减少了存储但是丢失了精度， 这在某些业务场景下是无法被接受的。
 * 下面的这种方法不仅可以减少存储，而且还可以做到完全准确，那就是使用 BitMap。
 * Bit-Map 的基本思想是用一个 bit 位来标记某个元素对应的 Value，而 Key 即是该元素。
 * 由于采用了 Bit 为单位来存储数据，因此可以大大节省存储空间。
 * 假设有这样一个需求：在 20 亿个随机整数中找出某个数 m 是否存在其中，并假设 32 位操作系统，4G 内存。
 * 在 Java 中，int 占 4 字节，1 字节 = 8 位（1 byte = 8 bit）如果每个数字用 int 存储，那就是 20 亿个 int，因而占用的空间约为
 * (2000000000*4/1024/1024/1024)≈7.45G如果按位存储就不一样了，20 亿个数就是 20 亿位，
 * 占用空间约为(2000000000/8/1024/1024/1024)≈0.233G在使用 BitMap 算法前，如果你需要去重的对象不是数字，那么需要先转换成数字。
 * 例如，用户可以自己创造一个映射器，将需要去重的对象和数字进行映射，最简单的办法是，可以直接使用数据库维度表中自增 ID。
 */
public class BitMapDistinct implements AggregateFunction<Long, Roaring64NavigableMap, Long> {
    @Override
    public Roaring64NavigableMap createAccumulator() {
        return new Roaring64NavigableMap();
    }

    @Override
    public Roaring64NavigableMap add(Long value, Roaring64NavigableMap accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public Long getResult(Roaring64NavigableMap accumulator) {
        return accumulator.getLongCardinality();
    }

    @Override
    public Roaring64NavigableMap merge(Roaring64NavigableMap a, Roaring64NavigableMap b) {
        return null;
    }
}
