package test;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;

public class FlinkJob {
    public static void main(String[] args) throws Exception {
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        final var tenv = StreamTableEnvironment.create(env);

        final var query = "select state, sum(amount) from txn t JOIN customer FOR SYSTEM_TIME AS OF t.ts ON t.cust_id = customer.id group by state, TUMBLE(t.ts, INTERVAL '1' SECOND)";

        var backend = new EmbeddedRocksDBStateBackend(true);
        backend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(backend);

        tenv.registerCatalog(TestCatalog.NAME, new TestCatalog());
        tenv.useCatalog(TestCatalog.NAME);

        tenv.toAppendStream(tenv.sqlQuery(query), RowData.class)
                .addSink(new DiscardingSink<>());
        env.execute();
    }
}

