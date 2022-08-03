package flink.join2;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.HashMap;
import java.util.List;

/**
 * Distributed Cache(分布式缓存)
 *
 * 实现方式
 *
 * 1. 通过env.registerCachedFile(cachedFilePath, cachedFileName)注册本地或HDFS缓存文件。
 * 2. 程序启动时，Flink会自动将文件分发到TaskManager文件系统中。
 * 3. 实现RichFlatMapFunction，在open()方法中通过RuntimeContext获取缓存文件并解析。
 * 4. 解析后的数据在内存中，此时可在flatMap()方法中实现维度关联。
 *
 * ● 注意
 *
 * 1. 由于数据会存储在内存中，因此，仅支持小数据量维表。
 * 2. 启动时加载，在维表变化时，需要重启任务。
 */
public class DistributedCacheJoinDim {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 注册缓存文件 如: file:///some/path 或 hdfs://host:port/and/path
        String cachedFilePath = "./user_info.txt";
        String cachedFileName = "user_info";
        env.registerCachedFile(cachedFilePath, cachedFileName);

        // 添加实时流
        DataStreamSource<Tuple2<String, String>> stream = env.fromElements(
                Tuple2.of("1", "click"),
                Tuple2.of("2", "click"),
                Tuple2.of("3", "browse"));

        // 关联维度
        SingleOutputStreamOperator<String> dimedStream = stream.flatMap(new RichFlatMapFunction<Tuple2<String, String>, String>() {

            HashMap dimInfo = new HashMap<String, Integer>();

            // 读取文件
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File cachedFile = getRuntimeContext().getDistributedCache().getFile(cachedFileName);
                List<String> lines = FileUtils.readLines(cachedFile);
                for (String line : lines) {
                    String[] split = line.split(",");
                    dimInfo.put(split[0], Integer.valueOf(split[1]));
                }
            }

            // 关联维度
            @Override
            public void flatMap(Tuple2<String, String> value, Collector<String> out) throws Exception {
                if (dimInfo.containsKey(value.f0)) {
                    Integer age = (Integer) dimInfo.get(value.f0);
                    out.collect(value.f0 + "," + value.f1 + "," + age);
                }
            }
        });

        dimedStream.print();
        env.execute();
    }
}
