package flink.sql.connector.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;

/**
 * 1. 应用场景：支持将 Watermark 的分配方式下推到 Source 中，比如 Kafka Source 中一个 Source Task 可以读取多个 Partition，Watermark 分配器下推到 Source 算子中后，就可以为每个 Partition 单独分配 Watermark Generator，这样 Watermark 的生成粒度就是 Kafka 的单 Partition，在事件时间下数据乱序会更小。
 * 2. ⭐ 支持之前：可以看到下图，Watermark 的分配是在 Source 节点之后的。
 * 3. ⭐ 支持方案及实现：在 DynamicTableSource 中实现 SupportsWatermarkPushDown 接口的方法，我们来看看 Flink Kafka Consumer 的具体实现方案：
 */
public class TableSourceWatermarkPushDown implements ScanTableSource, SupportsWatermarkPushDown {
    private WatermarkStrategy<RowData> watermarkStrategy;

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

    // 方法输入参数：
// WatermarkStrategy<RowData> watermarkStrategy：将用户 DDL 中的 watermark 生成方式传入
    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }
}
