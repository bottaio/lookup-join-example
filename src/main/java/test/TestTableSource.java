package test;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.List;

public class TestTableSource implements ScanTableSource {
    private final TestTable table;

    public TestTableSource(TestTable table) {
        this.table = table;
    }

    @Override
    public DynamicTableSource copy() {
        return new TestTableSource(table);
    }

    @Override
    public String asSummaryString() {
        return String.format("TestTableSource: %s", table.getName());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(StreamExecutionEnvironment env) {
                return env.addSource(new TestSource(table.getColumns(), table.getName().equals(CustomerTable.NAME),
                        table.getName().equals(CustomerTable.NAME) ? Config.DimensionRows : Config.FactRows));
            }

            @Override
            public boolean isBounded() {
                return true;
            }
        };
    }

    private static class TestSource implements SourceFunction<RowData> {
        private final List<TestInputFormat.ColumnSpec> columns;
        private final int numRows;
        private final boolean infiniteWatermark;
        private boolean running = true;
        private int cursor = 0;

        TestSource(List<TestInputFormat.ColumnSpec> columns, boolean infiniteWatermark, int numRows) {
            this.columns = columns;
            this.infiniteWatermark = infiniteWatermark;
            this.numRows = numRows;
        }

        @Override
        public void run(SourceContext<RowData> sourceContext) throws Exception {
            var row = new GenericRowData(columns.size());
            while (running && cursor <= numRows) {
                for (int i = 0; i < columns.size(); ++i) {
                    var col = columns.get(i);
                    row.setField(i, col.getValue(cursor));
                }
                sourceContext.collectWithTimestamp(row, infiniteWatermark ? Integer.MAX_VALUE : cursor);
                cursor += 1;
            }
        }

        @Override
        public void cancel() {
            running = true;
        }
    }
}
