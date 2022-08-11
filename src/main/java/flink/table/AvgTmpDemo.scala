package flink.table

import flink.sql.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression, Table}
import org.apache.flink.table.api.Expressions.{$, call}
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}

object AvgTmpDemo {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    val sensorTable: Table = tableEnv.fromDataStream(environment.addSource(new SensorSource), $("id"),$("timestamp"),$("temperature"))
    val avgTemp = new AvgTemp

    // Table API的调用
    val resultTable = sensorTable
      .groupBy($"id")
      .aggregate(avgTemp($"temperature") as "avgTemp")
      .select($"id", $"avgTemp")

    // SQL的实现
//    tableEnv.createTemporaryView("sensor", sensorTable)
//    tableEnv.registerFunction("avgTemp", new AvgTemp())
//    val resultSqlTable = tableEnv.sqlQuery(
//      """
//        |SELECT
//        |id, avgTemp(temperature)
//        |FROM
//        |sensor
//        |GROUP BY id
//  """.stripMargin)

    // 转换成流打印输出
    resultTable.toRetractStream[(String, Double)].print("agg temp")
//    resultSqlTable.toRetractStream[Row].print("agg temp sql")
    environment.execute()
  }
}
