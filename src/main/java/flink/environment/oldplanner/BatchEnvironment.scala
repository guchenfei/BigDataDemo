package flink.environment.oldplanner

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

class BatchEnvironment {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment: BatchTableEnvironment = BatchTableEnvironment.create(environment)
  }
}
