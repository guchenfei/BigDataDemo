package flink.table

import flink.sql.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.types.Row
import org.apache.flink.table.api._

/**
 * 5.4 聚合函数（Aggregate Functions）
 *用户自定义聚合函数（User-Defined Aggregate Functions，UDAGGs）可以把一个表中的数据，聚合成一个标量值。用户定义的聚合函数，是通过继承AggregateFunction抽象类实现的。
 *
 * 上图中显示了一个聚合的例子。
 *假设现在有一张表，包含了各种饮料的数据。该表由三列（id、name和price）、五行组成数据。现在我们需要找到表中所有饮料的最高价格，即执行max（）聚合，结果将是一个数值。
 * AggregateFunction的工作原理如下。
 *首先，它需要一个累加器，用来保存聚合中间结果的数据结构（状态）。可以通过调用AggregateFunction的createAccumulator()方法创建空累加器。
 *随后，对每个输入行调用函数的accumulate()方法来更新累加器。
 *处理完所有行后，将调用函数的getValue()方法来计算并返回最终结果。
 *  AggregationFunction要求必须实现的方法：
 *  createAccumulator()
 *  accumulate()
 *  getValue()
 *除了上述方法之外，还有一些可选择实现的方法。其中一些方法，可以让系统执行查询更有效率，而另一些方法，对于某些场景是必需的。例如，如果聚合函数应用在会话窗口（session group window）的上下文中，则merge()方法是必需的。
 *  retract()
 *  merge()
 *  resetAccumulator()
 *接下来我们写一个自定义AggregateFunction，计算一下每个sensor的平均温度值。
 */
object AvgTmpDemo {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    val sensorTable: Table = tableEnv.fromDataStream(environment.addSource(new SensorSource), $("id"),$("timestamp"),$("temperature"))
    val avgTemp = new AvgTemp

    // Table API的调用
//    val resultTable = sensorTable
//      .groupBy($"id")
//      .aggregate(avgTemp($"temperature") as "avgTemp")
//      .select($"id", $"avgTemp")

    // SQL的实现
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("avgTemp", avgTemp)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |SELECT
        |id, avgTemp(temperature)
        |FROM
        |sensor
        |GROUP BY id
  """.stripMargin)

    // 转换成流打印输出
//    resultTable.toRetractStream[(String, Double)].print("agg temp")
    resultSqlTable.toRetractStream[Row].print("agg temp sql") //table => stream
    environment.execute()
  }
}
