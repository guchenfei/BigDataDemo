package flink.sql.module;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HiveUDF extends UDF {
    public String evaluate(String input){
        return "Hello:" + input;
    }
}
