package flink.sql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * 大家会比较好奇，要写SQL就纯SQL呗，要写DataStream就纯DataStream呗，为哈还要把这两类接口做集成呢？
 * 博主举一个案例：在pdd这种发补贴券的场景下，希望可以在发的补贴券总金额超过1W元时，及时报警出来，来帮助
 * 控制预算，防止发的太多。
 * 对应的解决方案，我们可以想到使用SQL计算补贴券发放的结果，但是SQL的问题在于无法做到报警。所以我们可以将
 * SQL的查询的结果（即Table对象）转为DataStream，然后就可以在DataStream后自定义报警逻辑的算子
 * 我们直接上SQL和DataStreamAPI互相转化的案例
 */
public class SqlToDataStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);
        // 1. pdd 发补贴券流水数据
        String createTableSql = "CREATE TABLE source_table (\n"
                + "    id BIGINT,\n" //补贴券的流水id
        + "    money BIGINT,\n" // 补贴券的金额
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp_LTZ(3)),\n"
                + "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.id.min' = '1',\n"
                + "  'fields.id.max' = '100000',\n"
                + "  'fields.money.min' = '1',\n"
                + "  'fields.money.max' = '100000'\n"
                + ")\n";

        // 2. 计算总计发放补贴券的金额
        String querySql = "SELECT UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 as window_end, \n"
                + "      window_start, \n"
                + "      sum(money) as sum_money,\n" // 补贴券的发放总金额
                + "      count(distinct id) as count_distinct_id\n"
                + "FROM TABLE(CUMULATE(\n"
                + "         TABLE source_table\n"
                + "         , DESCRIPTOR(row_time)\n"
                + "         , INTERVAL '5' SECOND\n"
                + "         , INTERVAL '1' DAY))\n"
                + "GROUP BY window_start, \n"
                + "        window_end";

        tableEnvironment.executeSql(createTableSql);

        Table resultTable = tableEnvironment.sqlQuery(querySql);

        // 3. 将金额结果转为 DataStream，然后自定义超过 1w 的报警逻辑
        tableEnvironment.toAppendStream(resultTable, Row.class)
                .flatMap(new FlatMapFunction<Row, Object>() {
                    @Override
                    public void flatMap(Row value, Collector<Object> out) throws Exception {
                        long l = Long.parseLong(String.valueOf(value.getField(Integer.parseInt("sum_money"))));
                        if (l > 10000L) {
                            System.out.println("报警，超过 1w");
                        }
                    }
                });

        tableEnvironment.execute("");
    }
}
