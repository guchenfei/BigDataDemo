package flink.udf

import junit.framework.Test
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, dataStreamConversions, tableConversions}
import org.apache.flink.types.Row

import scala.collection.mutable

class FlattenDemo {

  def getStageBackend() = ???

  def testLateralTVF(): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnvironment = StreamTableEnvironment.create(environment, settings)
    environment.setStateBackend(getStageBackend())
    //    StreamITCase.clear
    val userData = new mutable.MutableList[(String)]
    userData.+=(("Sunny#8"))
    userData.+=(("Kevin#36"))
    userData.+=(("Panpan#36"))

//    val sqlQuery = "select data,name,age, from userTab,LATERAL(splitTVF(data)) as T(name,age)"
//    val users = environment.fromCollection(userData).toTable(tableEnvironment, $("data"))
//    val tVF = new SplitTVF()
//    tableEnvironment.registerTable("userTab", users)
//    tableEnvironment registerFunction("splitTVF", tVF)
//    val result = tableEnvironment.sqlQuery(sqlQuery).toAppendStream[Row]
    //    result.addSink(...)
    environment.execute()
  }
}
