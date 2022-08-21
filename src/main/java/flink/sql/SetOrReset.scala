package flink.sql

/**
 * Set子句用于修改一些Flink Sql环境的配置,Reset子句用于将所有的环境配置恢复成默认的配置
 * 只能在Sql Cli中进行使用,主要为了让用户更纯粹的使用Sql而不必使用其他方式或者切换系统环境
 * 语法:
 * SET (key = value)?
 *
 * RESET (key)?
 */
class SetOrReset {
  def main(args: Array[String]): Unit = {
    /**
     * 启动Sql Cli进行使用
     * Flink SQL> SET table.planner = blink;
     * [INFO] Session property has been set.
     *
     * Flink SQL> SET;
     * table.planner=blink;
     *
     * Flink SQL> RESET table.planner;
     * [INFO] Session property has been reset.
     *
     * Flink SQL> RESET;
     * [INFO] All session properties have been set to their default values.
     */
  }
}
