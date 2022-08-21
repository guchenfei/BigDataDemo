package flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Load加载Flink Sql体系内置的或者用户自定义的Module,UnLoad反向操作
 * 语法：
 * -- 加载
 * LOAD MODULE module_name [WITH ('key1' = 'val1', 'key2' = 'val2', ...)]
 *
 * -- 卸载
 * UNLOAD MODULE module_name
 */
object LoadOrUnLoad {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 加载 Flink SQL 体系内置的 Hive module
    tEnv.executeSql("LOAD MODULE hive WITH ('hive-version' = '3.1.2')")
    tEnv.executeSql("SHOW MODULES").print
    // +-------------+
    // | module name |
    // |        core |
    // |        hive |

    // 卸载唯一的一个 CoreModule
    tEnv.executeSql("UNLOAD MODULE core")
    tEnv.executeSql("SHOW MODULES").print
    //结果啥module也没了
  }
}
