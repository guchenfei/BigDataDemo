package flink.join2;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

/**
 * 随着 Flink Table & SQL的发展，Flink SQL中用于进行维表Join也成为了很多场景的选择。
 *
 * 基于之前的总结，再次总结下Flink Table & SQL 中维表Join的实现方式,包括DataStream中的维表Join。
 *
 * ● 定时加载维度数据
 * ● Distributed Cache(分布式缓存)
 * ● Async IO(异步IO)
 * ● Broadcast State(广播状态)
 * ● UDTF + LATERAL TABLE语法
 * ● LookupableTableSource
 *
 * 定时加载维度数据
 *
 * 实现方式
 *
 * ●  实现RichFlatMapFunction, 在open()方法中起个线程定时读取维度数据并加载到内存。
 * ●  在flatMap()方法中实现维度关联。
 *
 * ● 注意
 * 1. 由于数据会存储在内存中，因此，仅支持小数据量维表。
 * 2. 定时加载，仅适用于更新不太频繁的维表。
 */
public class DimRichFlatMapFunction extends RichFlatMapFunction<UserBrowseLog, Tuple2<UserBrowseLog, UserInfo>> {

    private final String url;
    private final String user;
    private final String passwd;
    private final Integer reloadInterval;

    private Connection connection;
    private final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    HashMap dimInfo = new HashMap<String, UserInfo>();

    public DimRichFlatMapFunction(String url, String user, String passwd, Integer reloadInterval) {
        this.url = url;
        this.user = user;
        this.passwd = passwd;
        this.reloadInterval = reloadInterval;
    }

    /**
     * 打开连接
     * 定时加载维度数据
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(JDBC_DRIVER);

        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    if (connection == null || connection.isClosed()) {
                        //log.warn("No connection. Trying to reconnect...");
                        connection = DriverManager.getConnection(url, user, passwd);
                    }
                    String sql = "select uid,name,age,address from t_user_info";
                    PreparedStatement preparedStatement = connection.prepareStatement(sql);
                    ResultSet resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        UserInfo userInfo = new UserInfo();
                        userInfo.setUid(resultSet.getString("uid"));
                        userInfo.setName(resultSet.getString("name"));
                        userInfo.setAge(resultSet.getInt("age"));
                        userInfo.setAddress(resultSet.getString("address"));

                        dimInfo.put(userInfo.getUid(), userInfo);
                    }
                } catch (SQLException e) {
//                    log.error("Get dimension data exception...", e);
                }
            }
        };

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(timerTask, 0, reloadInterval * 1000);

    }

    /**
     * 关闭连接
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * 维度关联
     * @param value
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap(UserBrowseLog value, Collector<Tuple2<UserBrowseLog, UserInfo>> out) throws Exception {
        String userID = value.getUserId();
        if (dimInfo.containsKey(userID)) {
            UserInfo dim = (UserInfo) dimInfo.get(userID);
            out.collect(new Tuple2<>(value, dim));
        }
    }
}
