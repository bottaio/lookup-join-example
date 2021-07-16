package test;

import org.apache.flink.table.catalog.CatalogDatabase;

import java.util.Map;
import java.util.Optional;

public class TestDatabase implements CatalogDatabase {
    @Override
    public Map<String, String> getProperties() {
        return Map.of();
    }

    @Override
    public String getComment() {
        return "";
    }

    @Override
    public CatalogDatabase copy() {
        return new TestDatabase();
    }

    @Override
    public CatalogDatabase copy(Map<String, String> properties) {
        return new TestDatabase();
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.empty();
    }
}
