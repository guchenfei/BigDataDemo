package flink.sourceOrSink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class ReadTextFileDemo {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    // read text file from local files system
     DataSet<String> localLines = env.readTextFile("file:///path/to/my/textfile");
     // read text file from an HDFS running at nnHost:nnPort
     DataSet<String> hdfsLines = env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile");
     // read a CSV file with three fields
    DataSet<Tuple3<Integer, String, Double>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
             .types(Integer.class, String.class, Double.class);
    // read a CSV file with five fields, taking only two of them
    DataSet<Tuple2<String, Double>> input = env.readCsvFile("hdfs:///the/CSV/file")
            .includeFields("10010")
        // take the first and the fourth field
    .types(String.class, Double.class);
    // read a CSV file with three fields into a POJO (Person.class) with corresponding fields
    DataSet<Person> input2 = env.readCsvFile("hdfs:///the/CSV/file")
            .pojoType(Person.class, "name", "age", "zipcode");
}
