package flink.table;


import com.alibaba.fastjson.JSON;
import flink.join2.ProductInfo;
import flink.join2.UserBrowseLog;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Summary:
 *  时态表(Temporal Table)
 */
public class Test {
    public static void main(String[] args) throws Exception{

        args=new String[]{"--application","flink/src/main/java/com/bigdata/flink/tableSqlTemporalTable/application.properties"};

        //1、解析命令行参数
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(fromArgs.getRequired("application"));

        //browse log
        String kafkaBootstrapServers = parameterTool.getRequired("kafkaBootstrapServers");
        String browseTopic = parameterTool.getRequired("browseTopic");
        String browseTopicGroupID = parameterTool.getRequired("browseTopicGroupID");

        //product history info
        String productInfoTopic = parameterTool.getRequired("productHistoryInfoTopic");
        String productInfoGroupID = parameterTool.getRequired("productHistoryInfoGroupID");

        //2、设置运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);
        streamEnv.setParallelism(1);

        //3、注册Kafka数据源
        //注意: 为了在北京时间和时间戳之间有直观的认识，这里的UserBrowseLog中增加了一个字段eventTimeTimestamp作为eventTime的时间戳
        Properties browseProperties = new Properties();
        browseProperties.put("bootstrap.servers",kafkaBootstrapServers);
        browseProperties.put("group.id",browseTopicGroupID);
        DataStream<UserBrowseLog> browseStream=streamEnv
                .addSource(new FlinkKafkaConsumer<>(browseTopic, new SimpleStringSchema(), browseProperties))
                .process(new BrowseKafkaProcessFunction())
                .assignTimestampsAndWatermarks(new BrowseTimestampExtractor(Time.seconds(0)));

        tableEnv.registerDataStream("browse",browseStream,"userID,eventTime,eventTimeTimestamp,eventType,productID,productPrice,browseRowtime.rowtime");
        //tableEnv.toAppendStream(tableEnv.scan("browse"),Row.class).print();

        //4、注册时态表(Temporal Table)
        //注意: 为了在北京时间和时间戳之间有直观的认识，这里的ProductInfo中增加了一个字段updatedAtTimestamp作为updatedAt的时间戳
        Properties productInfoProperties = new Properties();
        productInfoProperties.put("bootstrap.servers",kafkaBootstrapServers);
        productInfoProperties.put("group.id",productInfoGroupID);
        DataStream<ProductInfo> productInfoStream=streamEnv
                .addSource(new FlinkKafkaConsumer<>(productInfoTopic, new SimpleStringSchema(), productInfoProperties))
                .process(new ProductInfoProcessFunction())
                .assignTimestampsAndWatermarks(new ProductInfoTimestampExtractor(Time.seconds(0)));

        tableEnv.registerDataStream("productInfo",productInfoStream, "productID,productName,productCategory,updatedAt,updatedAtTimestamp,productInfoRowtime.rowtime");
        //设置Temporal Table的时间属性和主键
        TemporalTableFunction productInfo = tableEnv.scan("productInfo").createTemporalTableFunction("productInfoRowtime", "productID");
        //注册TableFunction
        tableEnv.registerFunction("productInfoFunc",productInfo);
        //tableEnv.toAppendStream(tableEnv.scan("productInfo"),Row.class).print();

        //5、运行SQL
        String sql = ""
                + "SELECT "
                + "browse.userID, "
                + "browse.eventTime, "
                + "browse.eventTimeTimestamp, "
                + "browse.eventType, "
                + "browse.productID, "
                + "browse.productPrice, "
                + "productInfo.productID, "
                + "productInfo.productName, "
                + "productInfo.productCategory, "
                + "productInfo.updatedAt, "
                + "productInfo.updatedAtTimestamp "
                + "FROM "
                + " browse, "
                + " LATERAL TABLE (productInfoFunc(browse.browseRowtime)) as productInfo "
                + "WHERE "
                + " browse.productID=productInfo.productID";

        Table table = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(table, Row.class).print();

        //6、开始执行
        tableEnv.execute(Test.class.getSimpleName());


    }


    /**
     * 解析Kafka数据
     */
    static class BrowseKafkaProcessFunction extends ProcessFunction<String, UserBrowseLog> {
        @Override
        public void processElement(String value, Context ctx, Collector<UserBrowseLog> out) throws Exception {
            try {

                UserBrowseLog log = JSON.parseObject(value, UserBrowseLog.class);

                // 增加一个long类型的时间戳
                // 指定eventTime为yyyy-MM-dd HH:mm:ss格式的北京时间
                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                OffsetDateTime eventTime = LocalDateTime.parse(log.getEventTimeStr(), format).atOffset(ZoneOffset.of("+08:00"));
                // 转换成毫秒时间戳
                long eventTimeTimestamp = eventTime.toInstant().toEpochMilli();
                log.setEventTime(eventTimeTimestamp);

                out.collect(log);
            }catch (Exception ex){
                //log.error("解析Kafka数据异常...",ex);
            }
        }
    }

    /**
     * 提取时间戳生成水印
     */
    static class BrowseTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserBrowseLog> {

        BrowseTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(UserBrowseLog element) {
            return element.getEventTime();
        }
    }


    /**
     * 解析Kafka数据
     */
    static class ProductInfoProcessFunction extends ProcessFunction<String, ProductInfo> {
        @Override
        public void processElement(String value, Context ctx, Collector<ProductInfo> out) throws Exception {
            try {

                ProductInfo log = JSON.parseObject(value, ProductInfo.class);

                // 增加一个long类型的时间戳
                // 指定eventTime为yyyy-MM-dd HH:mm:ss格式的北京时间
                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                OffsetDateTime eventTime = LocalDateTime.parse(log.getUpdateTimeStr(), format).atOffset(ZoneOffset.of("+08:00"));
                // 转换成毫秒时间戳
                long eventTimeTimestamp = eventTime.toInstant().toEpochMilli();
                log.setUpdateTime(eventTimeTimestamp);

                out.collect(log);
            }catch (Exception ex){
                //log.error("解析Kafka数据异常...",ex);
            }
        }
    }

    /**
     * 提取时间戳生成水印
     */
    static class ProductInfoTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<ProductInfo> {

        ProductInfoTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(ProductInfo element) {
            return element.getUpdateTime();
        }
    }

}
