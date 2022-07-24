package hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class nvl extends UDF {
    public Text evaluate(final Text x, final Text y) {
        if (x == null || x.toString().trim().length() == 0) {
            return y;
        }
        return x;
    }
}
