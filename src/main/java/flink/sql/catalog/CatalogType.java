package flink.sql.catalog;

/**
 * SQL 元数据扩展 - Catalog
 * Flink Catalog 功能介绍
 * 数据处理最关键的方面之一是管理元数据。元数据可以是临时的，例如临时表、UDF。 元数据也可以是持久化的，例如 Hive MetaStore 中的元数据。
 * Flink SQL 中是由 Catalog 提供了元数据信息，例如数据库、表、分区、视图以及数据库或其他外部系统中存储的函数和信息。对标 Hive 去理解就是 Hive 的 MetaStore，都是用于存储计算引擎涉及到的元数据信息。
 * Catalog 允许用户引用其数据存储系统中现有的元数据，并自动将其映射到 Flink 的相应元数据。例如，Flink 可以直接使用 Hive MetaStore 中的表的元数据，也可以将 Flink SQL 中的元数据存储到 Hive MetaStore 中。Catalog 极大地简化了用户开始使用 Flink 的步骤，提升了用户体验。
 * 目前 Flink 包含了以下四种 Catalog：
 * 1. ⭐ GenericInMemoryCatalog：GenericInMemoryCatalog 是基于内存实现的 Catalog，所有元数据只在 session 的生命周期（即一个 Flink 任务一次运行生命周期内）内可用。
 * 2. ⭐ JdbcCatalog：JdbcCatalog 使得用户可以将 Flink 通过 JDBC 协议连接到关系数据库。PostgresCatalog 是当前实现的唯一一种 JDBC Catalog，即可以将 Flink SQL 的预案数据存储在 Postgres 中。
 * 3. ⭐ HiveCatalog：HiveCatalog 有两个用途，作为 Flink 元数据的持久化存储，以及作为读写现有 Hive 元数据的接口。注意：Hive MetaStore 以小写形式存储所有元数据对象名称。而 GenericInMemoryCatalog 会区分大小写。
 * 4. ⭐ 用户自定义 Catalog：用户可以实现 Catalog 接口实现自定义 Catalog
 */
public class CatalogType {
    public static void main(String[] args) {
        //2.JdbcCatalog
        // PostgresCatalog 方法支持的方法
//        PostgresCatalog.databaseExists(String databaseName)
//        PostgresCatalog.listDatabases()
//        PostgresCatalog.getDatabase(String databaseName)
//        PostgresCatalog.listTables(String databaseName)
//        PostgresCatalog.getTable(ObjectPath tablePath)
//        PostgresCatalog.tableExists(ObjectPath tablePath)

        //3.HiveCatalog
        /**
         * TableEnvironment tableEnv = TableEnvironment.create(settings);
         *
         * String name            = "myhive";
         * String defaultDatabase = "mydatabase";
         * String hiveConfDir     = "/opt/hive-conf";
         *
         * HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
         * tableEnv.registerCatalog("myhive", hive);
         *
         * // set the HiveCatalog as the current catalog of the session
         * tableEnv.useCatalog("myhive");
         */
    }
}
