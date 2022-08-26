package flink.sql.connector.sink;

/**
 * 1. 应用场景：支持将 metadata 写入到 Sink 中。举例：可以往 Kafka Sink 中写入 Kafka 的 timestamp、header 等。案例可见 org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSink
 * 2. ⭐ 支持方案及实现：在 DynamicTableSink 中实现 SupportsWritingMetadata 接口的方法，我们来看看 KafkaDynamicSink 的具体实现方案：
 */
public class TableSinkWritingMetadata {
    /**
     * 1.  应用场景：支持将 metadata 写入到 Sink 中。举例：可以往 Kafka Sink 中写入 Kafka 的 timestamp、header 等。案例可见 org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSink
     * 2. ⭐ 支持方案及实现：在 DynamicTableSink 中实现 SupportsWritingMetadata 接口的方法，我们来看看 KafkaDynamicSink 的具体实现方案：
     */


    /**
     * // 注意！！！先执行 listWritableMetadata()，然后执行 applyWritableMetadata(xxx, xxx) 方法
     *
     * // 1. 方法返回参数 Map<String, DataType>：Flink 会获取到可以写入到 Kafka Sink 中的 metadata 都有哪些
     * @Override
     * public Map<String, DataType> listWritableMetadata() {
     *     final Map<String, DataType> metadataMap = new LinkedHashMap<>();
     *     Stream.of(WritableMetadata.values())
     *             .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
     *     return metadataMap;
     * }
     *
     * // 2. 方法输入参数：
     * // List<String> metadataKeys：通过解析用户的 SQL 语句，得出用户写出到 Sink 的 metadata 列信息，是 listWritableMetadata() 返回结果的子集
     * // DataType consumedDataType：写出到 Sink 字段的 DataType 类型信息，包括了写出的 metadata 列的类型信息（注意！！！metadata 列会被添加到最后一列）。
     * // 用户可以将这两个信息获取到，然后传入构造的 SinkFunction 中实现将对应字段写入 metadata 流程。
     * @Override
     * public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
     *     this.metadataKeys = metadataKeys;
     *     this.consumedDataType = consumedDataType;
     * }
     */

    //我个人理解这个是自定义TimeStamp和header等元数据信息,覆盖了kafka生成的对应元数据
    /**
     * CREATE TABLE KafkaSourceTable (
     *   `user_id` BIGINT,
     *   `item_id` BIGINT,
     *   `behavior` STRING
     * ) WITH (
     *   'connector' = 'kafka',
     *   'topic' = 'source_topic',
     *   'properties.bootstrap.servers' = 'localhost:9092',
     *   'properties.group.id' = 'testGroup',
     *   'scan.startup.mode' = 'earliest-offset',
     *   'value.format' = 'json'
     * );
     *
     * CREATE TABLE KafkaSinkTable (
     *   -- 1. 定义 kafka 中 metadata 的 timestamp 列
     *   `timestamp` TIMESTAMP_LTZ(3) METADATA,
     *   `user_id` BIGINT,
     *   `item_id` BIGINT,
     *   `behavior` STRING
     * ) WITH (
     *   'connector' = 'kafka',
     *   'topic' = 'sink_topic',
     *   'properties.bootstrap.servers' = 'localhost:9092',
     *   'properties.group.id' = 'testGroup',
     *   'scan.startup.mode' = 'earliest-offset',
     *   'value.format' = 'json'
     * );
     *
     * insert into KafkaSinkTable
     * select
     *     -- 2. 写入到 kafka 的 metadata 中的 timestamp
     *     cast(CURRENT_TIMESTAMP as TIMESTAMP_LTZ(3)) as `timestamp`
     *     , user_id
     *     , item_id
     *     , behavior
     * from KafkaSourceTable
     */
}
