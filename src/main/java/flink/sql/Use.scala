package flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 3.15.USE 子句
 * 1.★应用场景：如果熟悉MySQL的同学会非常熟悉这个子句，在MySQL中，USE 子句通常被用于切换库，那么在Flink SQL体系中，它的作用也是和MySQL中USE 子句的功能基本一致，用于切换Catalog， DataBase，使用Module
 * 2.★SQL语法标准：
 * ★切换Catalog
 * USE CATALOG catalog_name
 * ★使用Module
 * USE MODULES module_name1[, module_name2, ...]
 * ★切换Database
 * USE db名称
 * 案例如下
 */
object Use {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // create a catalog
    tEnv.executeSql("CREATE CATALOG cat1 WITH (...)")
    tEnv.executeSql("SHOW CATALOGS").print
    // +-----------------+
    // |    catalog name |
    // | default_catalog |
    // | cat1            |


    // change default catalog
    tEnv.executeSql("USE CATALOG cat1")

    tEnv.executeSql("SHOW DATABASES").print
    // databases are empty
    // +---------------+
    // | database name |


    // create a database
    tEnv.executeSql("CREATE DATABASE db1 WITH (...)")
    tEnv.executeSql("SHOW DATABASES").print
    // |        db1    |

    // change default database
    tEnv.executeSql("USE db1")

    // change module resolution order and enabled status
    tEnv.executeSql("USE MODULES hive")
    tEnv.executeSql("SHOW FULL MODULES").print
    // +-------------+-------+
    // | module name |  used |
    // |        hive |  true |
    // |        core | false |
  }
}
