package flink.sql.connector.sink;

/**
 * 1. 应用场景：（常用于批处理场景）支持类似于 Hive SQL 的 insert overwrite table xxx 的能力，将已有分区内的数据进行覆盖。
 * 2. ⭐ 支持方案及实现：在 DynamicTableSink 中实现 SupportsOverwrite 接口的方法，我们来看看 HiveTableSink 的具体实现方案：
 */
public class TableSinkOverWrite {
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
     * --- 2. 将 overwrite 字段设置到 FileSystemOutputFormat 中，在后续写入数据到 Hive 表时，如果 overwrite = true，则会覆盖直接覆盖已有数据
     * builder.setOverwrite(overwrite);
     * builder.setStaticPartitions(staticPartitionSpec);
     * ...
     * return dataStream
     *         .map((MapFunction<RowData, Row>) value -> (Row) converter.toExternal(value))
     *         .writeUsingOutputFormat(builder.build())
     *         .setParallelism(parallelism);
     * }
     *
     * // 1. 方法输入参数：
     * // boolean overwrite：用户写的 SQL 中如果包含了 overwrite 关键字，则方法入参 overwrite = true
     * // 如果不包含 overwrite 关键字，则方法入参 overwrite = false
     * @Override
     * public void applyOverwrite(boolean overwrite) {
     *     this.overwrite = overwrite;
     * }
     */


    //支持后的效果
    /**
     * insert overwrite hive_sink_table
     * select
     *     user_id
     *     , order_amount
     *     , server_timestamp_bigint
     *     , server_timestamp
     * from hive_source_table
     */
}
