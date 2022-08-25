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

        /**
         * 注意！！！
         * 1. ⭐ 此优化在窗口聚合中会自动生效，大家在使用 Window TVF 时可以看到 localagg + globalagg 两部分
         * 2. ⭐ 但是在 unbounded agg 中需要与 MiniBatch 参数相结合使用才会生效。
         */

        //3.split 分桶
        /**
         * ⭐ split 分桶如何解决上述问题：其核心思想在于按照 distinct 的 key，即 user_id，先做数据的分桶，将数据打散，分散到 Flink 的多个 TM 上进行计算，然后再将数据合桶计算。打开 split 分桶之后的效果就等同于以下 SQL：
         * 总体思想是先局部distinct然后总体求和
         */
        String sql = "SELECT color, SUM(cnt)\n" +
                "FROM (\n" +
                "    SELECT color, COUNT(DISTINCT user_id) as cnt\n" +
                "    FROM T\n" +
                "    GROUP BY color, MOD(HASH_CODE(user_id), 1024)\n" +
                ")\n" +
                "GROUP BY color";
        //⭐ 启用 split 分桶的参数：

        // 打开 split 分桶
        tableEnv.getConfig()
                .getConfiguration()
                .setString("table.optimizer.distinct-agg.split.enabled", "true");

        /**
         * 注意！！！
         * 1. ⭐ 如果有多个 distinct key，则多个 distinct key 都会被作为分桶 key。比如 count(distinct a)，sum(distinct b) 这种多个 distinct key 也支持。
         * 2. ⭐ 小伙伴萌自己写的 UDAF 不支持！
         * 3. ⭐ 其实此种优化很少使用，因为大家直接自己按照分桶的写法自己就可以写了，而且最后生成的算子图和自己写的 SQL 的语法也能对应的上
         */

        //4.去重 filter 子句
        //⭐ 问题场景：在一些场景下，用户可能需要从不同维度计算 UV，例如 Android 的 UV、iPhone 的 UV、Web 的 UV 和总 UV。许多用户会选择 CASE WHEN 支持此功能，如下 SQL 所示：
        String sql2 = "SELECT\n" +
                " day,\n" +
                " COUNT(DISTINCT user_id) AS total_uv,\n" +
                " COUNT(DISTINCT CASE WHEN flag IN ('android', 'iphone') THEN user_id ELSE NULL END) AS app_uv,\n" +
                " COUNT(DISTINCT CASE WHEN flag IN ('wap', 'other') THEN user_id ELSE NULL END) AS web_uv\n" +
                "FROM T\n" +
                "GROUP BY day";

        //但是如果你想实现类似的效果，Flink SQL 提供了更好性能的写法，就是本小节的 filter 子句。
        //⭐ Filter 子句重写上述场景：
        String sql3 = "SELECT\n" +
                " day,\n" +
                " COUNT(DISTINCT user_id) AS total_uv,\n" +
                " COUNT(DISTINCT user_id) FILTER (WHERE flag IN ('android', 'iphone')) AS app_uv,\n" +
                " COUNT(DISTINCT user_id) FILTER (WHERE flag IN ('web', 'other')) AS web_uv\n" +
                "FROM T\n" +
                "GROUP BY day";
        /**
         * Filter 子句的优化点在于，Flink 会识别出三个去重的 key 都是 user_id，因此会把三个去重的 key 存在一个共享的状态中。而不是上文 case when 中的三个状态中。其具体实现区别在于：
         *
         * ● ⭐ case when：total_uv、app_uv、web_uv 在去重时，state 是存在三个 MapState 中的，MapState key 为 user_id，value 为默认值，判断是否重复直接按照 key 是在 MapState 中的出现过进行判断。如果总 uv 为 1 亿，’android’, ‘iphone’ uv 为 5kw，’wap’, ‘other’ uv 为 5kw，则 3 个 state 要存储总共 2 亿条数据
         * ● ⭐ filter：total_uv、app_uv、web_uv 在去重时，state 是存在一个 MapState 中的，MapState key 为 user_id，value 为 long，其中 long 的第一个 bit 位标识在计算总 uv 时此 user_id 是否来光顾哦，第二个标识 ‘android’, ‘iphone’，第三个标识 ‘wap’, ‘other’，因此在上述 case when 相同的数据量的情况下，总共只需要存储 1 亿条数据，state 容量减小了几乎 50%
         *
         * 或者下面的场景也可以使用 filter 子句进行替换。
         */

        //⭐ 优化前：
        String sql4 = "select\n" +
                "    day\n" +
                "    , app_typp\n" +
                "    , count(distinct user_id) as uv\n" +
                "from source_table\n" +
                "group by\n" +
                "    day\n" +
                "    , app_type";

        //如果能够确定 app_type 是可以枚举的，比如为 android、iphone、web 三种，则可以使用 filter 子句做性能优化：
        String sql5 = "select\n" +
                "    day\n" +
                "    , count(distinct user_id) filter (where app_type = 'android') as android_uv\n" +
                "    , count(distinct user_id) filter (where app_type = 'iphone') as iphone_uv\n" +
                "    , count(distinct user_id) filter (where app_type = 'web') as web_uv\n" +
                "from source_table\n" +
                "group by\n" +
                "    day";
        //经过上述优化之后，state 大小的优化效果也会是成倍提升的。
    }
}
