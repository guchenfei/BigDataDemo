package flink.sql

/**
 * SQL Hints
 * 1.应用场景：比如有一个kafka数据源表 kafka＿table1，用户想直接从 latest-offset select一些数据出来预览，其 元数据已经存储在Hive MetaStore中，但是Hive MetaStore 中存储的配置中的 scan.startup.mode是earliest- offset，通过SQL Hints，用户可以在DML 语句中将 scan.startup.mode 改为latest-offset查询，因此可以看出
 * SQL Hints 常用语这种比较临时的参数修改，比如Ad—hoc这种临时查询中，方便用户使用自定义的新的表参数而不是Catalog中已有的表参数。
 * 2.SQL 语法标准：
 * 以下DML SQL中的
 * /*+OPTIONS(key=val[,key=val]*) */
 * 就是SQL Hints。
 *
 * SELECT *
 * FROM table_path /*+ OPTIONS(key=val [, key=val]*) */
 */
object SqlHints {
  /**
   * CREATE TABLE kafka_table1 (id BIGINT, name STRING, age INT) WITH (...);
   * CREATE TABLE kafka_table2 (id BIGINT, name STRING, age INT) WITH (...);
   *
   * -- 1. 使用 'scan.startup.mode'='earliest-offset' 覆盖原来的 scan.startup.mode
   * select id, name from kafka_table1 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */;
   *
   * -- 2. 使用 'scan.startup.mode'='earliest-offset' 覆盖原来的 scan.startup.mode
   * select * from
   * kafka_table1 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ t1
   * join
   * kafka_table2 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ t2
   * on t1.id = t2.id;
   *
   * -- 3. 使用 'sink.partitioner'='round-robin' 覆盖原来的 Sink 表的 sink.partitioner
   * insert into kafka_table1 /*+ OPTIONS('sink.partitioner'='round-robin') */ select * from kafka_table2;
   */
}
