package flink.sql.connector.source;

/**
 * 1. 应用场景：支持读取 Source 的 metadata，比如在 Kafka Source 中读取 Kafka 的 offset，写入时间戳等数据
 * 2. ⭐ 支持之前：比如想获取 Kafka 中的 offset 字段，在之前是不支持的
 * 3. ⭐ 支持方案及实现：在 DynamicTableSource 中实现 SupportsReadingMetadata 接口的方法，我们来看看 Flink Kafka Consumer 的具体实现方案：
 */
public class TableSourceReadingMetadata {
    /**
     * // 注意！！！先执行 listReadableMetadata()，然后执行 applyReadableMetadata(xxx, xxx) 方法
     *
     * // 方法输出参数：列出 Kafka Source 可以从 Kafka 中读取的 metadata 数据
     * @Override
     * public Map<String, DataType> listReadableMetadata() {
     *     final Map<String, DataType> metadataMap = new LinkedHashMap<>();
     *
     *     // add value format metadata with prefix
     *     valueDecodingFormat
     *             .listReadableMetadata()
     *             .forEach((key, value) -> metadataMap.put(VALUE_METADATA_PREFIX + key, value));
     *
     *     // add connector metadata
     *     Stream.of(ReadableMetadata.values())
     *             .forEachOrdered(m -> metadataMap.putIfAbsent(m.key, m.dataType));
     *
     *     return metadataMap;
     * }
     *
     * // 方法输入参数：
     * // List<String> metadataKeys：用户 SQL 中写入到 Sink 表的的 metadata 字段名称（metadataKeys）
     * // DataType producedDataType：将用户 SQL 写入到 Sink 表的所有字段的类型信息传进来，包括了 metadata 字段的类型信息
     * @Override
     * public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
     *     final List<String> formatMetadataKeys =
     *             metadataKeys.stream()
     *                     .filter(k -> k.startsWith(VALUE_METADATA_PREFIX))
     *                     .collect(Collectors.toList());
     *     final List<String> connectorMetadataKeys = new ArrayList<>(metadataKeys);
     *     connectorMetadataKeys.removeAll(formatMetadataKeys);
     *
     *     final Map<String, DataType> formatMetadata = valueDecodingFormat.listReadableMetadata();
     *     if (formatMetadata.size() > 0) {
     *         final List<String> requestedFormatMetadataKeys =
     *                 formatMetadataKeys.stream()
     *                         .map(k -> k.substring(VALUE_METADATA_PREFIX.length()))
     *                         .collect(Collectors.toList());
     *         valueDecodingFormat.applyReadableMetadata(requestedFormatMetadataKeys);
     *     }
     *
     *     this.metadataKeys = connectorMetadataKeys;
     *     this.producedDataType = producedDataType;
     * }
     */

    //支持之后的效果
    /**
     * CREATE TABLE KafkaTable (
     *    // METADATA 字段用于声明可以从 Source 读取的 metadata
     *    // 关于 Flink Kafka Source 可以读取的 metadata 见以下链接
     *    // https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/kafka/#available-metadata
     *   `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
     *   `partition` BIGINT METADATA VIRTUAL,
     *   `offset` BIGINT METADATA VIRTUAL,
     *   `user_id` BIGINT,
     *   `item_id` BIGINT,
     *   `behavior` STRING
     * ) WITH (
     *   'connector' = 'kafka',
     *   'topic' = 'user_behavior',
     *   'properties.bootstrap.servers' = 'localhost:9092',
     *   'properties.group.id' = 'testGroup',
     *   'scan.startup.mode' = 'earliest-offset',
     *   'format' = 'csv'
     * );
     */

    //在后续的 DML SQL 语句中就可以正常使用这些 metadata 字段的数据了。
}
