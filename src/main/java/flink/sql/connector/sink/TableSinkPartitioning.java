package flink.sql.connector.sink;

/**
 * 1. 应用场景：（常用于批处理场景）支持类似于 Hive SQL 的 insert INTO xxx partition(key = ‘A’) 的能力，支持将结果数据写入某个静态分区。
 * 2. ⭐ 支持方案及实现：在 DynamicTableSink 中实现 SupportsPartitioning 接口的方法，我们来看看 HiveTableSink 的具体实现方案：
 */
public class TableSinkPartitioning {
    /**
     * private DataStreamSink<Row> createBatchSink(
     *     DataStream<RowData> dataStream,
     *     DataStructureConverter converter,
     *     StorageDescriptor sd,
     *     HiveWriterFactory recordWriterFactory,
     *     OutputFileConfig fileNaming,
     *     final int parallelism)
     *     throws IOException {
     * FileSystemOutputFormat.Builder<Row> builder = new FileSystemOutputFormat.Builder<>();
     * ...
     * builder.setMetaStoreFactory(msFactory());
     * builder.setOverwrite(overwrite);
     * --- 2. 将 staticPartitionSpec 字段设置到 FileSystemOutputFormat 中，在后续写入数据到 Hive 表时，如果有静态分区，则会将数据写入到对应的静态分区中
     * builder.setStaticPartitions(staticPartitionSpec);
     * ...
     * return dataStream
     *         .map((MapFunction<RowData, Row>) value -> (Row) converter.toExternal(value))
     *         .writeUsingOutputFormat(builder.build())
     *         .setParallelism(parallelism);
     * }
     *
     * // 1. 方法输入参数：
     * // Map<String, String> partitionMap：用户写的 SQL 中如果包含了 partition(partition_key = 'A') 关键字
     * // 则方法入参 Map<String, String> partitionMap 的输入值转为 JSON 后为：{"partition_key": "A"}
     * // 用户可以自己将方法入参的 partitionMap 保存到自定义变量中，后续写出到 Hive 表时进行使用
     * @Override
     * public void applyStaticPartition(Map<String, String> partitionMap) {
     *     staticPartitionSpec = new LinkedHashMap<>();
     *     for (String partitionCol : getPartitionKeys()) {
     *         if (partitionMap.containsKey(partitionCol)) {
     *             staticPartitionSpec.put(partitionCol, partitionMap.get(partitionCol));
     *         }
     *     }
     * }
     */

    /**
     * insert overwrite hive_sink_table partition(date = '2022-01-01')
     * select
     *     user_id
     *     , order_amount
     *     , server_timestamp_bigint
     *     , server_timestamp
     * from hive_source_table
     */
}
