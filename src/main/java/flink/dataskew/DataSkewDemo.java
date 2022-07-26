package flink.dataskew;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.eclipse.jetty.util.ajax.JSON;

import java.util.Random;

public class DataSkewDemo {
//    DataStream sourceStream = ;
//    resultStream = sourceStream.map(record -> {
//        Record record = JSON.parseObject(record, Record.class);
//        String type = record.getType();
//        record.setType(type + "#" + new Random().nextInt(100));
//        return record;
//    })
//      .keyBy(0)
//    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
//            .aggregate(new CountAggregate())
//            .map(count -> {
//                String key = count.getKey.substring(0, count.getKey.indexOf("#"));
//                return RecordCount(key,count.getCount);      })
//    //二次聚合
//    .keyBy(0)
//    .process(new CountProcessFunction);
//    resultStream.sink()...
//            env.execute()...;
}
