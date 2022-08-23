package flink.sql.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class CatalogApi {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        //1.使用 SQL API 将表创建注册进 Catalog
        // 创建 HiveCatalog
        Catalog catalog = new HiveCatalog("myhive", null, "<path_of_hive_conf>");
        // 注册 catalog
        tableEnv.registerCatalog("myhive", catalog);
        // 在 catalog 中创建 database
        tableEnv.executeSql("CREATE DATABASE mydb WITH (...)");
        // 在 catalog 中创建表
        tableEnv.executeSql("CREATE TABLE mytable (name STRING, age INT) WITH (...)");
        tableEnv.listTables(); // 列出当前 myhive.mydb 中的所有表

        //2.使用 Java API 将表创建注册进 Catalog
        /**
         *
         * TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
         *
         * // 创建 HiveCatalog
         * Catalog catalog = new HiveCatalog("myhive", null, "<path_of_hive_conf>");
         *
         * // 注册 catalog
         * tableEnv.registerCatalog("myhive", catalog);
         *
         * // 在 catalog 中创建 database
         * catalog.createDatabase("mydb", new CatalogDatabaseImpl(...));
         *
         * // 在 catalog 中创建表
         * TableSchema schema = TableSchema.builder()
         *     .field("name", DataTypes.STRING())
         *     .field("age", DataTypes.INT())
         *     .build();
         *
         * catalog.createTable(
         *         new ObjectPath("mydb", "mytable"),
         *         new CatalogTableImpl(
         *             schema,
         *             new Kafka()
         *                 .version("0.11")
         *                 ....
         *                 .startFromEarlist()
         *                 .toProperties(),
         *             "my comment"
         *         ),
         *         false
         *     );
         *
         * List<String> tables = catalog.listTables("mydb"); // 列出当前 myhive.mydb 中的所有表
         */

        //3.操作 Catalog 的 Java API,Sql DDL在DDl部分
        //3.1 Catalog 操作
        /**
         * // 注册 Catalog
         * tableEnv.registerCatalog(new CustomCatalog("myCatalog"));
         *
         * // 切换 Catalog 和 Database
         * tableEnv.useCatalog("myCatalog");
         * tableEnv.useDatabase("myDb");
         * // 也可以通过以下方式访问对应的表
         * tableEnv.from("not_the_current_catalog.not_the_current_db.my_table");
         */

        //3.2 数据库操作
        /**
         * // create database
         * catalog.createDatabase("mydb", new CatalogDatabaseImpl(...), false);
         *
         * // drop database
         * catalog.dropDatabase("mydb", false);
         *
         * // alter database
         * catalog.alterDatabase("mydb", new CatalogDatabaseImpl(...), false);
         *
         * // get databse
         * catalog.getDatabase("mydb");
         *
         * // check if a database exist
         * catalog.databaseExists("mydb");
         *
         * // list databases in a catalog
         * catalog.listDatabases("mycatalog");
         */

        //3.3 表操作
        /**
         * // create table
         * catalog.createTable(new ObjectPath("mydb", "mytable"), new CatalogTableImpl(...), false);
         *
         * // drop table
         * catalog.dropTable(new ObjectPath("mydb", "mytable"), false);
         *
         * // alter table
         * catalog.alterTable(new ObjectPath("mydb", "mytable"), new CatalogTableImpl(...), false);
         *
         * // rename table
         * catalog.renameTable(new ObjectPath("mydb", "mytable"), "my_new_table");
         *
         * // get table
         * catalog.getTable("mytable");
         *
         * // check if a table exist or not
         * catalog.tableExists("mytable");
         *
         * // list tables in a database
         * catalog.listTables("mydb");
         */

        //3.4 视图操作
        /**
         * // create view
         * catalog.createTable(new ObjectPath("mydb", "myview"), new CatalogViewImpl(...), false);
         *
         * // drop view
         * catalog.dropTable(new ObjectPath("mydb", "myview"), false);
         *
         * // alter view
         * catalog.alterTable(new ObjectPath("mydb", "mytable"), new CatalogViewImpl(...), false);
         *
         * // rename view
         * catalog.renameTable(new ObjectPath("mydb", "myview"), "my_new_view", false);
         *
         * // get view
         * catalog.getTable("myview");
         *
         * // check if a view exist or not
         * catalog.tableExists("mytable");
         *
         * // list views in a database
         * catalog.listViews("mydb");
         */

        //3.5 分区操作
        /**
         * // create view
         * catalog.createPartition(
         *     new ObjectPath("mydb", "mytable"),
         *     new CatalogPartitionSpec(...),
         *     new CatalogPartitionImpl(...),
         *     false);
         *
         * // drop partition
         * catalog.dropPartition(new ObjectPath("mydb", "mytable"), new CatalogPartitionSpec(...), false);
         *
         * // alter partition
         * catalog.alterPartition(
         *     new ObjectPath("mydb", "mytable"),
         *     new CatalogPartitionSpec(...),
         *     new CatalogPartitionImpl(...),
         *     false);
         *
         * // get partition
         * catalog.getPartition(new ObjectPath("mydb", "mytable"), new CatalogPartitionSpec(...));
         *
         * // check if a partition exist or not
         * catalog.partitionExists(new ObjectPath("mydb", "mytable"), new CatalogPartitionSpec(...));
         *
         * // list partitions of a table
         * catalog.listPartitions(new ObjectPath("mydb", "mytable"));
         *
         * // list partitions of a table under a give partition spec
         * catalog.listPartitions(new ObjectPath("mydb", "mytable"), new CatalogPartitionSpec(...));
         *
         * // list partitions of a table by expression filter
         * catalog.listPartitionsByFilter(new ObjectPath("mydb", "mytable"), Arrays.asList(epr1, ...));
         */

        //3.6 函数操作
        /**
         * // create function
         * catalog.createFunction(new ObjectPath("mydb", "myfunc"), new CatalogFunctionImpl(...), false);
         *
         * // drop function
         * catalog.dropFunction(new ObjectPath("mydb", "myfunc"), false);
         *
         * // alter function
         * catalog.alterFunction(new ObjectPath("mydb", "myfunc"), new CatalogFunctionImpl(...), false);
         *
         * // get function
         * catalog.getFunction("myfunc");
         *
         * // check if a function exist or not
         * catalog.functionExists("myfunc");
         *
         * // list functions in a database
         * catalog.listFunctions("mydb");
         */
    }
}
