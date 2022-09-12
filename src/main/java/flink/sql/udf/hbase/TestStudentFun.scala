package flink.sql.udf.hbase

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Expressions.call
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression, Table}
import org.apache.flink.types.Row

object TestStudentFun {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    val rowKeySource: DataStream[String] = environment.fromCollection(Seq("2015001", "2015002"))
    val myTable: Table = tableEnv.fromDataStream(rowKeySource, $"rowKey")
    //Sql api 调用UDF
    //    val table: Table = myTable.select($"rowKey")
    //    tableEnv.toAppendStream[Row](table).print()
    val table: Table = myTable.select(call(classOf[StudentInfoFunction], $"rowKey"))
    tableEnv.toAppendStream[Row](table).print()

    /**
     * 6> +I[+I[lishi, 20, 160, 50, female, 13, 4, yes, 48, 69, 80]]
     * 5> +I[+I[Zhangsan, 18, 174, 60, male, 13, 4, yes, 88, 99, 110]]
     */
    environment.execute()
  }
}
