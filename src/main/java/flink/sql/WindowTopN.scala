package flink.sql

/**
 *
 * WindowTopN
 * WindowTopN定义（支持Streaming)，WindowTopN是一种特殊的TopN,它的返回结果是每一个窗囗内的
 * N个最小值或者最大值。
 *
 * 应用场景:“小伙伴会问了，我有了TopN为啥还需要WindowTopN呢？还记得上文介绍TopN说道的TopN
 * 时会出现中间结果，从而出现回撤数据的嘛？WindowTopN不会出现回撤数据，因为WindowTopN实现是在窗囗
 * 结束输出最终结果，不会产生中间结果。而且注意，因为是窗囗上面的操作，WindowTOPN在窗囗结束时，会自
 * 动把State给清除。
 *
 * SQL语法标准
 * SELECT [column_list]
 * FROM (
 * SELECT [column_list],
 * ROW_NUMBER() OVER (PARTITION BY window_start, window_end [, col_key1...]
 * ORDER BY col1 [asc|desc][, col2 [asc|desc]...]) AS rownum
 * FROM table_name) -- windowing TVF
 * WHERE rownum <= N [AND conditions]
 *
 * 案例:取当前这一分钟搜索关键词下搜索热度前10名词条数据
 */
object WindowTopN {
  def main(args: Array[String]): Unit = {
    /**
     * -- 字段名	        备注
     * -- key         	    搜索关键词
     * -- name             搜索热度名称
     * -- search_cnt       热搜消费热度（比如 3000）
     * -- timestamp        消费词条时间戳
     *
     * CREATE TABLE source_table (
     * name BIGINT NOT NULL,
     * search_cnt BIGINT NOT NULL,
     * key BIGINT NOT NULL,
     * row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),
     * WATERMARK FOR row_time AS row_time
     * ) WITH (
     * ...
     * );
     *
     * -- 输出表字段：
     * -- 字段名	        备注
     * -- key              搜索关键词
     * -- name        	    搜索热度名称
     * -- search_cnt       热搜消费热度（比如 3000）
     * -- window_start     窗口开始时间戳
     * -- window_end       窗口结束时间戳
     *
     * CREATE TABLE sink_table (
     * key BIGINT,
     * name BIGINT,
     * search_cnt BIGINT,
     * window_start TIMESTAMP(3),
     * window_end TIMESTAMP(3)
     * ) WITH (
     * ...
     * );
     *
     * -- 处理 sql：
     *
     * INSERT INTO sink_table
     * SELECT key, name, search_cnt, window_start, window_end
     * FROM (
     * SELECT key, name, search_cnt, window_start, window_end,
     * ROW_NUMBER() OVER (PARTITION BY window_start, window_end, key
     * ORDER BY search_cnt desc) AS rownum
     * FROM (
     * SELECT window_start, window_end, key, name, max(search_cnt) as search_cnt
     * -- window tvf 写法
     * FROM TABLE(TUMBLE(TABLE source_table, DESCRIPTOR(row_time), INTERVAL '1' MINUTES))
     * GROUP BY window_start, window_end, key, name
     * )
     * )
     * WHERE rownum <= 100
     *
     * 示例结果
     * +I[关键词1, 词条1, 8670, 2021-1-28T22:34, 2021-1-28T22:35]
     * +I[关键词1, 词条2, 6928, 2021-1-28T22:34, 2021-1-28T22:35]
     * +I[关键词1, 词条3, 1735, 2021-1-28T22:34, 2021-1-28T22:35]
     * +I[关键词1, 词条4, 7287, 2021-1-28T22:34, 2021-1-28T22:35]
     * ...
     *
     *
     * SQL语义
     * 数据源:数据源即最新的词条下面的搜索词的搜索热度数据，消费到Kafka中数据后，将数据按照窗囗聚合的
     * key通过hash分发策略发送到下游窗囗聚合算子
     *
     * 窗口聚合算子:进行窗囗聚合计算，随着时间的推进，将窗囗聚合结果计算完成发往下游窗囗排序算子
     *
     * 窗口排序算子:这个算子其实也是一个窗囗算子，只不过这个窗囗子为每个Key维护了一个TopN的榜单数
     * 接受到上游发送的窗囗结果数据进行排序，随着时间的推进，窗囗的结束，将排序的结果输出到下游数据汇算子
     *
     * 数据汇:接收到上游的数据之后，然后输出到外部存储引擎中
     */
  }
}
