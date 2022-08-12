package flink.table

import flink.sql.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression, Table}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.table.api._

/**
 * 表聚合函数（Table Aggregate Functions）
用户定义的表聚合函数（User-Defined Table Aggregate Functions，UDTAGGs），可以把一个表中数据，聚合为具有多行和多列的结果表。这跟AggregateFunction非常类似，只是之前聚合结果是一个标量值，现在变成了一张表。
比如现在我们需要找到表中所有饮料的前2个最高价格，即执行top2()表聚合。我们需要检查5行中的每一行，得到的结果将是一个具有排序后前2个值的表。
用户定义的表聚合函数，是通过继承TableAggregateFunction抽象类来实现的。
TableAggregateFunction的工作原理如下。
首先，它同样需要一个累加器（Accumulator），它是保存聚合中间结果的数据结构。通过调用TableAggregateFunction的createAccumulator()方法可以创建空累加器。
随后，对每个输入行调用函数的accumulate()方法来更新累加器。
处理完所有行后，将调用函数的emitValue()方法来计算并返回最终结果。
AggregationFunction要求必须实现的方法：
createAccumulator()
accumulate()
除了上述方法之外，还有一些可选择实现的方法。
retract()
merge()
resetAccumulator()
emitValue()
emitUpdateWithRetract()
接下来我们写一个自定义TableAggregateFunction，用来提取每个sensor最高的两个温度值。
 */
object Top2TmpDemo {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    val sensorTable: Table = tableEnv.fromDataStream(environment.addSource(new SensorSource), $("id"),$("timestamp"),$("temperature"))
    val top2Temp = new Top2Temp
    val resultTable: Table = sensorTable.groupBy($"id")
      .flatAggregate(top2Temp($"temperature") as ("temp", "rank"))
      .select($"id", $"temp", $"rank")

    //轉化成劉打印輸出
    resultTable.toRetractStream[(String,Double,Int)].print("agg tmp")
    environment.execute("Top2TmpDemo")
  }
}
