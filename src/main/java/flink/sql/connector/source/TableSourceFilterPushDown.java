package flink.sql.connector.source;

import com.google.common.collect.Lists;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import java.util.LinkedList;
import java.util.List;

/**
 * 1. 应用场景：将 where 中的一些过滤条件下推到 Source 中进行处理，这样不需要的数据就可以不往下游发送了，性能会有提升。
 * 2. ⭐ 优化前：如下图 web ui 算子图，过滤条件都在 Source 节点之后有单独的 filter 算子进行承接
 *
 * 3. ⭐ 优化方案及实现：在 DynamicTableSource 中实现 SupportsFilterPushDown 接口的方法，具体实现方案如下：
 */
public class TableSourceFilterPushDown implements ScanTableSource
        , SupportsFilterPushDown  {
        private List<ResolvedExpression> filters;

        // 方法输入参数：List<ResolvedExpression> filters：引擎下推过来的过滤条件，然后在此方法中来决定哪些条件需要被下推
        // 方法输出参数：Result：Result 记录哪些过滤条件在 Source 中应用，哪些条件不能在 Source 中应用
        @Override
        public Result applyFilters(List<ResolvedExpression> filters) {
            this.filters = new LinkedList<>(filters);

            // 1.不上推任何过滤条件
            // Result.of(上推的 filter, 没有做上推的 filter)
//        return Result.of(Lists.newLinkedList(), filters);
            // 2.将所有的过滤条件都上推到 source
            return Result.of(filters, Lists.newLinkedList());
        }

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

}
