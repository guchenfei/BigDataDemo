package flink.sql.udf.hbase;

import org.apache.flink.metrics.Counter;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * 该UDF展示了从hbase两张表获取关联信息后返回一条数据的过程
 * 表信息:
 * hbase 准备两张表
 * <p>
 * 学生信息表
 * student
 * basic_info列簇(姓名name，年龄age，身高height，体重weight，性别sex)
 * class_info列簇(年级grade，班级class，在籍状态state)
 * <p>
 * put 'student','2015001','basic_info:name','Zhangsan';
 * put 'student','2015001','basic_info:age','18';
 * put 'student','2015001','basic_info:height','174';
 * put 'student','2015001','basic_info:weight','60';
 * put 'student','2015001','basic_info:sex','male';
 * put 'student','2015001','class_info:grade','13';
 * put 'student','2015001','class_info:class','4';
 * put 'student','2015001','class_info:state','yes';
 * <p>
 * put 'student','2015002','basic_info:name','lishi';
 * put 'student','2015002','basic_info:age','20';
 * put 'student','2015002','basic_info:height','160';
 * put 'student','2015002','basic_info:weight','50';
 * put 'student','2015002','basic_info:sex','female';
 * put 'student','2015002','class_info:grade','13';
 * put 'student','2015002','class_info:class','4';
 * put 'student','2015002','class_info:state','yes';
 * <p>
 * <p>
 * <p>
 * 成绩信息表
 * score
 * course列簇(语文Chinese，数学math，英语english)
 * put 'score','2015001','course:Chinese','88'
 * put 'score','2015001','course:math','99'
 * put 'score','2015001','course:english','110'
 * <p>
 * put 'score','2015002','course:Chinese','48'
 * put 'score','2015002','course:math','69'
 * put 'score','2015002','course:english','80'
 */
@FunctionHint(
        input = @DataTypeHint("STRING"),
        output = @DataTypeHint("Row<name STRING,age STRING,height STRING,weight STRING,sex STRING,grade STRING,class STRING,state STRING,Chinese STRING,math STRING,english STRING>")
)
public class StudentInfoFunction extends ScalarFunction {
    private static final Logger LOG = LoggerFactory.getLogger(StudentInfoFunction.class);
    private transient Counter hbaseInputRecords;
    private transient Counter hbaseOutputRecords;

    //student表的列簇信息
    private final String FAMILY_STUDENT_BASIC_INFO = "basic_info";
    private final String FAMILY_STUDENT_CLASS_INFO = "class_info";
    private final String FAMILY_SCORE_COURSE_INFO = "course";
    private final String tableNameStudent = "student";
    private final String tableNameScore = "score";
    private Connection connection = null;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        LOG.info("init hbase connection ...");
        //创建配置
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "Linux121");
        //hbase主默认端口
        configuration.set("hbase.master", "Linux121:60000");
        //zk客户端端口号
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(configuration);
        LOG.info("hbase connection {}", connection);
        /**
         * 自定义metrics，计数输入和输出条数
         */
        hbaseInputRecords =
                context.getMetricGroup().addGroup("gs_hbase_records").counter("hbaseInputRecords");
        hbaseOutputRecords =
                context.getMetricGroup().addGroup("gs_hbase_records").counter("hbaseOutputRecords");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }


    public Row eval(String rowKey) {
        if (rowKey.isEmpty()) {
            return null;
        }

        Row row = new Row(11);
        try {
            hbaseInputRecords.inc();
            //开始执行查询
            Table tableStudent = connection.getTable(TableName.valueOf(tableNameStudent));
            Table tableScore = connection.getTable(TableName.valueOf(tableNameScore));

            //先查第一张表
            Get getStudent = new Get(rowKey.getBytes(StandardCharsets.UTF_8));
//        getStudent.addColumn(FAMILY_STUDENT_BASIC_INFO.getBytes(), "name".getBytes());
//        getStudent.addColumn(FAMILY_STUDENT_BASIC_INFO.getBytes(), "age".getBytes());
//        getStudent.addColumn(FAMILY_STUDENT_BASIC_INFO.getBytes(), "height".getBytes());
//        getStudent.addColumn(FAMILY_STUDENT_BASIC_INFO.getBytes(), "weight".getBytes());
//        getStudent.addColumn(FAMILY_STUDENT_BASIC_INFO.getBytes(), "sex".getBytes());
//        getStudent.addColumn(FAMILY_STUDENT_CLASS_INFO.getBytes(), "grade".getBytes());
//        getStudent.addColumn(FAMILY_STUDENT_CLASS_INFO.getBytes(), "class".getBytes());
//        getStudent.addColumn(FAMILY_STUDENT_CLASS_INFO.getBytes(), "state".getBytes());

            Result resultStudent = tableStudent.get(getStudent);
            List<Cell> cells = resultStudent.listCells();
            if (cells.isEmpty()) {
                System.out.println("query student info is empty : rowKey" + rowKey);
                return null;
            }
            Iterator<Cell> iterator = cells.iterator();
            while (iterator.hasNext()) {
                Cell cell = iterator.next();
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
//            System.out.println("student info :family : " + family + ", column : " + column + " value :" + value);
                switch (column) {
                    case "name":
                        row.setField(0, value);
                        break;
                    case "age":
                        row.setField(1, value);
                        break;
                    case "height":
                        row.setField(2, value);
                        break;
                    case "weight":
                        row.setField(3, value);
                        break;
                    case "sex":
                        row.setField(4, value);
                        break;
                    case "grade":
                        row.setField(5, value);
                        break;
                    case "class":
                        row.setField(6, value);
                        break;
                    case "state":
                        row.setField(7, value);
                        break;
                    default:
                        break;
                }
            }

            //再查第二张表
            Get getScore = new Get(rowKey.getBytes(StandardCharsets.UTF_8));
//        getScore.addColumn(FAMILY_SCORE_COURSE_INFO.getBytes(), "Chinese".getBytes());
//        getScore.addColumn(FAMILY_SCORE_COURSE_INFO.getBytes(), "math".getBytes());
//        getScore.addColumn(FAMILY_SCORE_COURSE_INFO.getBytes(), "english".getBytes());

            Result resultScore = tableScore.get(getScore);
            List<Cell> cellsScore = resultScore.listCells();
            Iterator<Cell> scoreIterator = cellsScore.iterator();
            while (scoreIterator.hasNext()) {
                Cell cell = scoreIterator.next();
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                //System.out.println("score info :family : " + family + " , column : " + column + " value : " + value);
                switch (column) {
                    case "Chinese":
                        row.setField(8, value);
                        break;
                    case "math":
                        row.setField(9, value);
                        break;
                    case "english":
                        row.setField(10, value);
                        break;
                    default:
                        break;
                }
            }
            //返回合并的数据 如果是拼接字符串 用它分割\\u0001
            hbaseOutputRecords.inc();
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return row;
    }
}
