package flink.hive

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._

object TestHiveStreaming {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tableEnv :StreamTableEnvironment = StreamTableEnvironment.create(env,settings)

    val stream = env
      .fromElements(
        ("10", "haha"),
        ("11", "hehe")
      )

    val name            = "myhive"
    val defaultDatabase = "mydb"
    val hiveConfDir     = "/Users/yuanzuo/Downloads/apache-hive-3.1.2-bin/conf" // a local path
    val version         = "3.1.2"

    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
    tableEnv.registerCatalog(name, hive)

    // set the HiveCatalog as the current catalog of the session
    tableEnv.useCatalog(name)
    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    tableEnv.useDatabase(defaultDatabase)
    tableEnv.createTemporaryView("users", stream, 'id, 'name)
    tableEnv.executeSql("insert into t_user select id, name from users")
    tableEnv.executeSql("select * from t_user")//t_user表属于hive

    env.execute()
  }
}