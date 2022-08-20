package flink.sql

object CollectionsOperator {
  def main(args: Array[String]): Unit = {
    //示例展示
    /**
     * 第一类:
     * UNION 集合合并去重
     * UNION ALL集合合不去重
     *
     * Flink SQL> create view t1(s) as values ('c'), ('a'), ('b'), ('b'), ('c');
     * Flink SQL> create view t2(s) as values ('d'), ('e'), ('a'), ('b'), ('b');
     *
     * Flink SQL> (SELECT s FROM t1) UNION (SELECT s FROM t2);
     * +---+
     * |  s|
     * +---+
     * |  c|
     * |  a|
     * |  b|
     * |  d|
     * |  e|
     * +---+
     *
     * Flink SQL> (SELECT s FROM t1) UNION ALL (SELECT s FROM t2);
     * +---+
     * |  c|
     * +---+
     * |  c|
     * |  a|
     * |  b|
     * |  b|
     * |  c|
     * |  d|
     * |  e|
     * |  a|
     * |  b|
     * |  b|
     * +---+
     *
     * 第二类:
     * Intersect:交集去重
     * Intersect ALL:交集不去重
     *
     * Flink SQL> create view t1(s) as values ('c'), ('a'), ('b'), ('b'), ('c');
     * Flink SQL> create view t2(s) as values ('d'), ('e'), ('a'), ('b'), ('b');
     * Flink SQL> (SELECT s FROM t1) INTERSECT (SELECT s FROM t2);
     * +---+
     * |  s|
     * +---+
     * |  a|
     * |  b|
     * +---+
     *
     * Flink SQL> (SELECT s FROM t1) INTERSECT ALL (SELECT s FROM t2);
     * +---+
     * |  s|
     * +---+
     * |  a|
     * |  b|
     * |  b|
     * +---+
     *
     * 第三类:
     * Except：差集去重
     * Except ALL：差集不去重
     *
     * Flink SQL> (SELECT s FROM t1) EXCEPT (SELECT s FROM t2);
     * +---+
     * | s |
     * +---+
     * | c |
     * +---+
     *
     * Flink SQL> (SELECT s FROM t1) EXCEPT ALL (SELECT s FROM t2);
     * +---+
     * | s |
     * +---+
     * | c |
     * | c |
     * +---+
     *
     *
     * 上述SQL在流式任务中，如果一条左流数据先来了，没有从右流集合数据中找到对应的数据时会直接输出，当右流对应的
     * 数据后续来了之后，会下发回撤流将之前的数据给撤回。这也是一个回撤流。
     * In子查询：这个大家比较熟悉了，但是注意，In子查询的结果集只能有一列
     *
     * SELECT user, amount
     * FROM Orders
     * WHERE product IN (
     * SELECT product FROM NewProducts
     * )
     * 注意In子查询大状态问题,注意设置state ttl
     */
  }
}
