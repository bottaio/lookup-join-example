package test;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.AbstractCatalogTable;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;

import java.util.*;

public abstract class TestTable extends AbstractCatalogTable {
    public TestTable(TableSchema tableSchema, Map<String, String> options, String comment) {
        super(tableSchema, options, comment);
    }

    public abstract String getName();

    public abstract List<TestInputFormat.ColumnSpec> getColumns();
}

class TxnTable extends TestTable {
    public static final String NAME = "txn";

    private static final TableSchema schema = TableSchema.builder()
            .field("ts", DataTypes.TIMESTAMP(3))
            .field("prod_id", DataTypes.BIGINT())
            .field("cust_id", DataTypes.BIGINT())
            .field("type", DataTypes.BIGINT())
            .field("amount", DataTypes.BIGINT())
            .field("items", DataTypes.BIGINT())
            .watermark("ts", "ts", DataTypes.TIMESTAMP(3))
            .build();

    @Override
    public List<TestInputFormat.ColumnSpec> getColumns() {
        return Arrays.asList(
                new TestInputFormat.TimestampColumn("ts"),
                new TestInputFormat.LongColumnSpec("prod_id", Config.DimensionRows),
                new TestInputFormat.LongColumnSpec("cust_id", Config.DimensionRows),
                new TestInputFormat.LongColumnSpec("type", Config.DimensionRows),
                new TestInputFormat.LongColumnSpec("amount", Config.DimensionRows ),
                new TestInputFormat.LongColumnSpec("items", Config.DimensionRows)
        );
    }

    public TxnTable() {
        super(schema, Collections.emptyMap(), "");
    }

    @Override
    public CatalogTable copy(Map<String, String> map) {
        return new TxnTable();
    }

    @Override
    public CatalogBaseTable copy() {
        return new TxnTable();
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.empty();
    }

    @Override
    public String getName() {
        return NAME;
    }
}

class CustomerTable extends TestTable {
    public static final String NAME = "customer";

    private static final TableSchema schema = TableSchema.builder()
            .field("ts", DataTypes.TIMESTAMP(3))
            .field("id", DataTypes.BIGINT().notNull())
            .field("state", DataTypes.STRING())
            .field("age", DataTypes.BIGINT())
            .field("score", DataTypes.BIGINT())
            .watermark("ts", "ts", DataTypes.TIMESTAMP(3))
            .primaryKey("id")
            .build();

    @Override
    public List<TestInputFormat.ColumnSpec> getColumns() {
        return Arrays.asList(
                new TestInputFormat.TimestampColumn("ts"),
                new TestInputFormat.LongColumnSpec("id", Config.DimensionRows),
                new TestInputFormat.StringColumnSpec("state", "state-", Config.DimensionRows),
                new TestInputFormat.LongColumnSpec("age", Config.DimensionRows),
                new TestInputFormat.LongColumnSpec("age", Config.DimensionRows)
        );
    }

    public CustomerTable() {
        super(schema, Collections.emptyMap(), "");
    }

    @Override
    public CatalogTable copy(Map<String, String> map) {
        return new CustomerTable();
    }

    @Override
    public CatalogBaseTable copy() {
        return new CustomerTable();
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.empty();
    }

    @Override
    public String getName() {
        return NAME;
    }
}