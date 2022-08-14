package flink.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class Top3ScoreDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env,settings);

        SingleOutputStreamOperator<PlayerData> mapStream = env.readTextFile("src/main/data/score.csv").map(new MapFunction<String, PlayerData>() {
            @Override
            public PlayerData map(String value) throws Exception {
                String[] split = value.split(",");
                return new PlayerData(split[0],split[1],split[2],Integer.valueOf(split[3]),Double.valueOf(split[4]),Double.valueOf(split[5]),Double.valueOf(split[6]),Double.valueOf(split[7]),Double.valueOf(split[8]));
            }
        });

        Table table = tableEnvironment.fromDataStream(mapStream);
        tableEnvironment.createTemporaryView("score", table);
        String sql = "select player,count(season) as num from score group by player order by num desc limit 3";
        Table table1 = tableEnvironment.sqlQuery(sql);
        tableEnvironment.createTemporaryView("table1",table1);
//        tableEnvironment.toRetractStream(table1, Row.class).print();
        String[] fieldNames = {"name","num"};
       TypeInformation[] fieldTypes = {Types.STRING, Types.BIG_INT};
        TableSink<Row> configure = new CsvTableSink("/Users/guchenfei/IdeaProjects/BigDataDemo/src/main/data/result", ",")
                .configure(fieldNames, fieldTypes);
        tableEnvironment.registerTableSink("result",configure);
        tableEnvironment.sqlQuery("select * from table1").executeInsert("result");
        env.execute();
    }
}
