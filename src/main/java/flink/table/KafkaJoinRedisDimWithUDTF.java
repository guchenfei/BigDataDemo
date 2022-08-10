package flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Summary:
 * Kafka Join Redis-Dim
 */
public class KafkaJoinRedisDimWithUDTF {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        // Source DDL
        // Kafka数据: {"userID":"user_1","eventType":"click","eventTime":"2015-01-01 00:00:00"}
        String sourceDDL = ""
                + "create table source_kafka "
                + "( "
                + "    userID String, "
                + "    eventType String, "
                + "    eventTime String "
                + ") with ( "
                + "    'connector.type' = 'kafka', "
                + "    'connector.version' = '0.10', "
                + "    'connector.properties.bootstrap.servers' = 'kafka01:9092', "
                + "    'connector.properties.zookeeper.connect' = 'kafka01:2181', "
                + "    'connector.topic' = 'test_1', "
                + "    'connector.properties.group.id' = 'c1_test_1', "
                + "    'connector.startup-mode' = 'latest-offset', "
                + "    'format.type' = 'json' "
                + ")";
        tableEnv.sqlUpdate(sourceDDL);
        tableEnv.toAppendStream(tableEnv.from("source_kafka"), Row.class).print();

        // UDTF DDL
        // Redis中的数据 userID userName,userAge
        // 127.0.0.1:6379> get user_1
        // "name1,10"
        String udtfDDL = ""
                + "CREATE TEMPORARY FUNCTION "
                + "  IF NOT EXISTS UDTFRedis "
                + "  AS 'com.bigdata.flink.dimJoin.UDTFRedis'";
        tableEnv.sqlUpdate(udtfDDL);

        // Query
        // Left Join
        String execSQL = ""
                + "select "
                + " source_kafka.*,dim.* "
                + "from source_kafka "
                + "LEFT JOIN LATERAL TABLE(UDTFRedis(userID)) as dim (userName,userAge) ON TRUE";
        Table table = tableEnv.sqlQuery(execSQL);
        tableEnv.toAppendStream(table, Row.class).print();

        tableEnv.execute(KafkaJoinRedisDimWithUDTF.class.getSimpleName());
    }
}
