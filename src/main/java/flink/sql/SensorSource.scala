package flink.sql

import org.apache.flink.streaming.api.functions.source.SourceFunction

class SensorSource extends SourceFunction[Sensor]{
  override def run(sourceContext: SourceFunction.SourceContext[Sensor]): Unit = {}

  override def cancel(): Unit = {}
}
