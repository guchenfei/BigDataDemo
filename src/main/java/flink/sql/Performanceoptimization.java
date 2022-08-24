package flink.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 本小节主要介绍 Flink SQL 中的聚合算子的优化，在某些场景下应用这些优化后，性能提升会非常大。本小节主要包含以下四种优化：
 * 1. ⭐ （常用）MiniBatch 聚合：unbounded group agg 中，可以使用 minibatch 聚合来做到微批计算、访问状态、输出结果，避免每来一条数据就计算、访问状态、输出一次结果，从而减少访问 state 的时长（尤其是 Rocksdb）提升性能。
 * 2. ⭐ （常用）两阶段聚合：类似 MapReduce 中的 Combiner 的效果，可以先在 shuffle 数据之前先进行一次聚合，减少 shuffle 数据量
 * 3. ⭐ （不常用）split 分桶：在 count distinct、sum distinct 的去重的场景中，如果出现数据倾斜，任务性能会非常差，所以如果先按照 distinct key 进行分桶，将数据打散到各个 TM 进行计算，然后将分桶的结果再进行聚合，性能就会提升很大
 * 4. ⭐ （常用）去重 filter 子句：在 count distinct 中使用 filter 子句于 Hive SQL 中的 count(distinct if(xxx, user_id, null)) 子句，但是 state 中同一个 key 会按照 bit 位会进行复用，这对状态大小优化非常有用
 */
public class Performanceoptimization {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        Configuration configuration = tableEnv.getConfig().getConfiguration();
        /**
         * ⭐ table.exec.mini-batch.allow-latency 和 table.exec.mini-batch.size 两者只要其中一项满足条件就会执行 batch 访问状态操作。
         * ⭐ 上述 MiniBatch 配置不会对 Window TVF 生效，因为！！！Window TVF 默认就会启用小批量优化，Window TVF 会将 buffer 的输入记录记录在托管内存中，而不是 JVM 堆中，因此 Window TVF 不会有 GC 过高或者 OOM 的问题。
         */
        //1.启用 MiniBatch 聚合的参数
        configuration.setString("table.exec.mini-batch.enabled", "true");
        //buffer 最多 5s 的输入数据记录
        configuration.setString("table.exec.mini-batch.allow-latency", "5s");
        // buffer 最多的输入数据记录数目
        configuration.setString("table.exec.mini-batch.size", "5000");

        //2.启用两阶段聚合的参数：
        /**
         * 注意！！！
         * ⭐ 此优化在窗口聚合中会自动生效，大家在使用 Window TVF 时可以看到 localagg + globalagg 两部分
         * ⭐ 但是在 unbounded agg 中需要与 MiniBatch 参数相结合使用才会生效。
         */
        // 打开 miniBatch
        configuration.setString("table.exec.mini-batch.enabled", "true");
        configuration.setString("table.exec.mini-batch.allow-latency", "5s");
        configuration.setString("table.exec.mini-batch.size", "5000");
        // 打开两阶段聚合
        configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");


    }
}
