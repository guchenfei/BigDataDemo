package flink.sql.connector.source;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;

/**
 * limit 下推到Source
 * 1. ⭐ 应用场景：将 limit 子句下推到 Source 中，在批场景中可以过滤大部分不需要的数据
 * 2. ⭐ 优化前：如下图 web ui 算子图，limit 条件都在 Source 节点之后有单独的 Limit 算子进行承接
 * 3. ⭐ 优化方案及实现：在 DynamicTableSource 中实现 SupportsLimitPushDown 接口的方法，具体实现方案如下：
 */
public class TableSourceLimitPushDown implements ScanTableSource , SupportsLimitPushDown {
    private long limit = -1;

    @Override
    public ChangelogMode getChangelogMode() {
        return null;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        return null;
    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }

    /**
     * @param limit 引擎下推过来的limit条数
     */
    @Override
    public void applyLimit(long limit) {
        //将limit数接收到之后,然后在SourceFunction中可以进行过滤
        this.limit = limit;
    }
}
