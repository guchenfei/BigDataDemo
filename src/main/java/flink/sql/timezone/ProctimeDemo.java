package flink.sql.timezone;

/**
 * 关于处理时间系统会返回TIMESTAMP_LTZ类型的时间戳类型,因此会随时区变动
 */
public class ProctimeDemo {
    public static void main(String[] args) {
        //使用方式只用示例表示
        /**
         * Flink SQL> SET table.local-time-zone=UTC;
         * Flink SQL> SELECT PROCTIME();
         *
         * +-------------------------+
         * |              PROCTIME() |
         * +-------------------------+
         * | 2021-04-15 14:48:31.387 |
         * +-------------------------+
         *
         * Flink SQL> SET table.local-time-zone=Asia/Shanghai;
         * Flink SQL> SELECT PROCTIME();
         *
         * +-------------------------+
         * |              PROCTIME() |
         * +-------------------------+
         * | 2021-04-15 22:48:31.387 |
         * +-------------------------+
         *
         * Flink SQL> CREATE TABLE MyTable1 (
         *                   item STRING,
         *                   price DOUBLE,
         *                   proctime as PROCTIME()
         *             ) WITH (
         *                 'connector' = 'socket',
         *                 'hostname' = '127.0.0.1',
         *                 'port' = '9999',
         *                 'format' = 'csv'
         *            );
         *
         * Flink SQL> CREATE VIEW MyView3 AS
         *             SELECT
         *                 TUMBLE_START(proctime, INTERVAL '10' MINUTES) AS window_start,
         *                 TUMBLE_END(proctime, INTERVAL '10' MINUTES) AS window_end,
         *                 TUMBLE_PROCTIME(proctime, INTERVAL '10' MINUTES) as window_proctime,
         *                 item,
         *                 MAX(price) as max_price
         *             FROM MyTable1
         *                 GROUP BY TUMBLE(proctime, INTERVAL '10' MINUTES), item;
         *
         * Flink SQL> DESC MyView3;
         *
         * +-----------------+-----------------------------+-------+-----+--------+-----------+
         * |           name  |                        type |  null | key | extras | watermark |
         * +-----------------+-----------------------------+-------+-----+--------+-----------+
         * |    window_start |                TIMESTAMP(3) | false |     |        |           |
         * |      window_end |                TIMESTAMP(3) | false |     |        |           |
         * | window_proctime | TIMESTAMP_LTZ(3) *PROCTIME* | false |     |        |           |
         * |            item |                      STRING | true  |     |        |           |
         * |       max_price |                      DOUBLE | true  |     |        |           |
         * +-----------------+-----------------------------+-------+-----+--------+-----------+
         */

        //结果
        /**
         * Flink SQL> SET table.local-time-zone=UTC;
         * Flink SQL> SELECT * FROM MyView3;
         *
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         * |            window_start |              window_end |          window_procime | item | max_price |
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:10:00.005 |    A |       1.8 |
         * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:10:00.007 |    B |       2.5 |
         * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:10:00.007 |    C |       3.8 |
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         *
         * Flink SQL> SET table.local-time-zone=Asia/Shanghai;
         * Flink SQL> SELECT * FROM MyView3;
         *
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         * |            window_start |              window_end |          window_procime | item | max_price |
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         * | 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:10:00.005 |    A |       1.8 |
         * | 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:10:00.007 |    B |       2.5 |
         * | 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:10:00.007 |    C |       3.8 |
         * +-------------------------+-------------------------+-------------------------+------+-----------+
         */
    }
}
