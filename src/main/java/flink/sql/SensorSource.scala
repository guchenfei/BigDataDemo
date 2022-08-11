package flink.sql

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class SensorSource extends SourceFunction[Sensor]{
  override def run(sourceContext: SourceFunction.SourceContext[Sensor]): Unit = {
    val random = new Random()
    while (true){
      sourceContext.collect(Sensor(random.nextInt(10).toString,System.currentTimeMillis(),random.nextInt(40).toString))
    }
  }

  override def cancel(): Unit = {}
}
