package test;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class TestInputFormat implements InputFormat<RowData, TestInputFormat.Split> {
    private final List<ColumnSpec> columns;
    private final int maxRecords;
    private int cursor = 0;

    public TestInputFormat(int maxRecords, List<ColumnSpec> columns) {
        this.maxRecords = maxRecords;
        this.columns = columns;
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }

    @Override
    public Split[] createInputSplits(int i) throws IOException {
        return new Split[]{new Split()};
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(Split[] splits) {
        return new DefaultInputSplitAssigner(splits);
    }

    @Override
    public void open(Split split) throws IOException {

    }

    @Override
    public boolean reachedEnd() throws IOException {
        return cursor >= maxRecords;
    }

    @Override
    public RowData nextRecord(RowData rowData) throws IOException {
        if (rowData == null) {
            rowData = new GenericRowData(columns.size());
        }
        var row = (GenericRowData) rowData;
        for (int i = 0; i < columns.size(); ++i) {
            var col = columns.get(i);
            row.setField(i, col.getValue(cursor));
        }
        cursor++;
        return rowData;
    }

    @Override
    public void close() throws IOException {

    }

    public interface ColumnSpec extends Serializable {
        Object getValue(int cursor);
    }

    public static class LongColumnSpec implements ColumnSpec {
        private final int max;

        public LongColumnSpec(String name) {
            this(name, Integer.MAX_VALUE);
        }

        public LongColumnSpec(String name, int max) {
            this.max = max;
        }

        @Override
        public Object getValue(int cursor) {
            return (long) (cursor % max);
        }
    }

    public static class TimestampColumn implements ColumnSpec {
        public TimestampColumn(String name) {
        }

        @Override
        public Object getValue(int cursor) {
            return TimestampData.fromEpochMillis(cursor);
        }
    }

    public static class StringColumnSpec implements ColumnSpec {
        private final String prefix;
        private final int max;

        public StringColumnSpec(String name, String prefix) {
            this(name, prefix, Integer.MAX_VALUE);
        }

        public StringColumnSpec(String name, String prefix, int max) {
            this.prefix = prefix;
            this.max = max;
        }

        @Override
        public Object getValue(int cursor) {
            return StringData.fromString(prefix + (cursor % max));
        }
    }

    public static class Split implements InputSplit {

        @Override
        public int getSplitNumber() {
            return 0;
        }
    }
}
