package flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * show 常用于查询库,表,函数
 * 语法：
 * SHOW CATALOGS：展示所有 Catalog
 * SHOW CURRENT CATALOG：展示当前的 Catalog
 * SHOW DATABASES：展示当前 Catalog 下所有 Database
 * SHOW CURRENT DATABASE：展示当前的 Database
 * SHOW TABLES：展示当前 Database 下所有表
 * SHOW VIEWS：展示所有视图
 * SHOW FUNCTIONS：展示所有的函数
 * SHOW MODULES：展示所有的 Module（Module 是用于 UDF 扩展）
 */
object Show {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // show catalogs
    tEnv.executeSql("SHOW CATALOGS").print
    // +-----------------+
    // |    catalog name |
    // | default_catalog |


    // show current catalog
    tEnv.executeSql("SHOW CURRENT CATALOG").print
    // +----------------------+
    // | current catalog name |
    // |      default_catalog |


    // show databases
    tEnv.executeSql("SHOW DATABASES").print
    // +------------------+
    // |    database name |
    // | default_database |


    // show current database
    tEnv.executeSql("SHOW CURRENT DATABASE").print
    // +-----------------------+
    // | current database name |
    // |      default_database |


    // create a table
    tEnv.executeSql("CREATE TABLE my_table (...) WITH (...)")
    // show tables
    tEnv.executeSql("SHOW TABLES").print
    // +------------+
    // | table name |
    // |   my_table |


    // create a view
    tEnv.executeSql("CREATE VIEW my_view AS ...")
    // show views
    tEnv.executeSql("SHOW VIEWS").print
    // +-----------+
    // | view name |
    // |   my_view |


    // show functions
    tEnv.executeSql("SHOW FUNCTIONS").print
    // +---------------+
    // | function name |
    // |           mod |
    // |        sha256 |
    // |           ... |


    // create a user defined function
    tEnv.executeSql("CREATE FUNCTION f1 AS ...")
    // show user defined functions
    tEnv.executeSql("SHOW USER FUNCTIONS").print
    // |            f1 |


    // show modules
    tEnv.executeSql("SHOW MODULES").print
    // +-------------+
    // | module name |
    // |        core |


    // show full modules
    tEnv.executeSql("SHOW FULL MODULES").print
    // +-------------+-------+
    // | module name |  used |
    // |        core |  true |
    // |        hive | false |
  }
}
