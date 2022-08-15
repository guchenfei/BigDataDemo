package flink.environment.blinkplanner

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

class StreamEnvironment {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    /**
     * 1.14版本支持。1.14版本中，流和批的都统一到了StreamTableEnvironment中，因此就可以做Table和DataStream
     * 的互相转换了。
     */
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)
  }
}
