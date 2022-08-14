package flink.environment.blinkplanner

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

class BatchEnvironment {
  def main(args: Array[String]): Unit = {
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tableEnvironment: TableEnvironment = TableEnvironment.create(settings)
  }
}
