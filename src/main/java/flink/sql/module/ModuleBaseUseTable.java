package flink.sql.module;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.module.hive.HiveModule;

public class ModuleBaseUseTable {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

// Show initially loaded and enabled modules
        tableEnv.listModules();
// +-------------+
// | module name |
// +-------------+
// |        core |
// +-------------+
        tableEnv.listFullModules();
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        core | true |
// +-------------+------+

// Load a hive module
        tableEnv.loadModule("hive", new HiveModule());

// Show all enabled modules
        tableEnv.listModules();
// +-------------+
// | module name |
// +-------------+
// |        core |
// |        hive |
// +-------------+

// Show all loaded modules with both name and use status
        tableEnv.listFullModules();
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        core | true |
// |        hive | true |
// +-------------+------+

// Change resolution order
        tableEnv.useModules("hive", "core");
        tableEnv.listModules();
// +-------------+
// | module name |
// +-------------+
// |        hive |
// |        core |
// +-------------+
        tableEnv.listFullModules();
// +-------------+------+
// | module name | used |
// +-------------+------+
// |        hive | true |
// |        core | true |
// +-------------+------+

// Disable core module
        tableEnv.useModules("hive");
        tableEnv.listModules();
// +-------------+
// | module name |
// +-------------+
// |        hive |
// +-------------+
        tableEnv.listFullModules();
// +-------------+-------+
// | module name |  used |
// +-------------+-------+
// |        hive |  true |
// |        core | false |
// +-------------+-------+

// Unload hive module
        tableEnv.unloadModule("hive");
        tableEnv.listModules();
// Empty set
        tableEnv.listFullModules();
// +-------------+-------+
// | module name |  used |
// +-------------+-------+
// |        hive | false |
// +-------------+-------+
    }
}
