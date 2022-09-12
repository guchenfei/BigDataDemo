package flink.sql.connector.source;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DecodingFormatFactory;

/**
 * 1. ⭐ 应用场景：将下游用到的字段下推到 Source 中，然后 Source 中可以做到只取这些字段，不使用的字段就不往下游发
 * 2. ⭐ 优化前：如下图 web ui 算子图，limit 条件都在 Source 节点之后有单独的 Limit 算子进行承接
 */
public class TableSourceProjectionPushDown implements ScanTableSource, SupportsProjectionPushDown {
    private TableSchema tableSchema;

    @Override
    public ChangelogMode getChangelogMode() {
        return null;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        //create runtime classes that are shipped to the cluster
        /**
         *  final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
         *                 runtimeProviderContext,
         *                 getSchemaWithMetadata(this.tableSchema).toRowDataType());
         */
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

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
//        this.tableSchema = projectSchemaWithMetadata(this.tableSchema,projectedFields);
    }
}
