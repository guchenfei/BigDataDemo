package flink.join;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import java.util.Properties;

public class JoinDemo7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定是EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        env.setParallelism(1);

        //Kafka的ip和要消费的topic,//Kafka设置
        String kafkaIPs = "Linux121:9092,Linux122:9092,Linux123:9092";
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaIPs);
        props.setProperty("group.id", "group.cyb.2");

        /**
         * {"cityId":1000,"ts":1L,"userName":"user0"}
         * {"cityId":1000,"ts":5L,"userName":"user1"}
         * {"cityId":2000,"ts":8L,"userName":"user2"}
         * {"cityId":2000,"ts":10L,"userName":"user2"}
         *
         * {"cityId":1000,"cityName":"北京","ts":1L}
         * {"cityId":2000,"cityName":"运城","ts":5L}
         * {"cityId":3000,"cityName":"晋中","ts":6L}
         * {"cityId":4000,"cityName":"忻州","ts":10L}
         *
         * kafka-topics.sh --zookeeper localhost:2181/myKafka --create --topic city --partitions 2 --replication-factor 1 --config cleanup.policy=compact
         * kafka-console-producer.sh --broker-list 172.16.131.130:9000 --topic user
         */
        //读取用户信息Kafka
        FlinkKafkaConsumer<UserInfo> userConsumer = new FlinkKafkaConsumer<UserInfo>("user", new UserInfoSchema(), props);
        userConsumer.setStartFromEarliest();
        userConsumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserInfo>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(UserInfo userInfo) {
                return userInfo.getTs();
            }
        });

        //读取城市维度信息Kafka
        FlinkKafkaConsumer<CityInfo> cityConsumer = new FlinkKafkaConsumer<CityInfo>("city", new CityInfoSchema(), props);
        cityConsumer.setStartFromEarliest();
        cityConsumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CityInfo>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(CityInfo cityInfo) {
                return cityInfo.getTs();
            }
        });

        //主流，用户流, 格式为：user_name、city_id、ts
        Table userTable = tableEnv.fromDataStream(env.addSource(userConsumer),"userName,cityId,ts.rowtime" );
        //定义城市维度流,格式为：city_id、city_name、ts
        Table cityTable = tableEnv.fromDataStream(env.addSource(cityConsumer),"cityId,cityName,ts.rowtime");
        tableEnv.createTemporaryView("userTable", userTable);
        tableEnv.createTemporaryView("cityTable", cityTable);

        //定义一个TemporalTableFunction
        TemporalTableFunction dimCity = cityTable.createTemporalTableFunction("ts", "cityId");
        //注册表函数
        tableEnv.registerFunction("dimCity", dimCity);

        Table u = tableEnv.sqlQuery("select * from userTable");
        u.printSchema();
        tableEnv.toAppendStream(u, Row.class).print("用户流接收到：");

        Table c = tableEnv.sqlQuery("select * from cityTable");
        c.printSchema();
        tableEnv.toAppendStream(c, Row.class).print("城市流接收到：");

        //关联查询
        Table result = tableEnv
                .sqlQuery("select u.userName,u.cityId,d.cityName,u.ts " +
                        "from userTable as u " +
                        ", Lateral table  (dimCity(u.ts)) d " +
                        "where u.cityId=d.cityId");

        //打印输出
        DataStream resultDs = tableEnv.toAppendStream(result, Row.class);
        resultDs.print("\t\t关联输出：");
        env.execute("joinDemo");
    }
}
