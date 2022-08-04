package flink.table;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;

/**
 * UDTF
 */
public class UDTFRedis extends TableFunction<Row> {

    private Jedis jedis;

    /**
     * 打开连接
     * @param context
     * @throws Exception
     */
    @Override
    public void open(FunctionContext context) throws Exception {
        jedis = new Jedis("localhost", 6379);
        jedis.select(0);
    }

    /**
     * 关闭连接
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
        }
    }

    /**
     * 从Redis中查找维度数据
     * @param key
     */
    public void eval(String key) {
        String value = jedis.get(key);
        if (value != null) {
            String[] valueSplit = value.split(",");
            Row row = new Row(2);
            row.setField(0, valueSplit[0]);
            row.setField(1, Integer.valueOf(valueSplit[1]));
            collector.collect(row);
        }
    }

    /**
     * 定义返回的数据类型，返回数据为userName,userAge，所以这里为String,Int。
     * @return
     */
    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(Types.STRING, Types.INT);
    }
}
