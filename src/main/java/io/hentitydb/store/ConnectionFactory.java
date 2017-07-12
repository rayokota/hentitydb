package io.hentitydb.store;

import io.hentitydb.Configuration;

import java.util.concurrent.ExecutorService;

public interface ConnectionFactory extends AutoCloseable {

    /**
     * Returns the column store configuration.
     *
     * @return the column store configuration
     */
    Configuration getConfiguration();

    /**
     * Declares metadata for a table.
     *
     * @param <K> the row key type
     * @param <C> the column type
     * @param metadata the table metadata
     */
    <K, C> void declareTable(TableMetadata<K, C> metadata);

    /**
     * Returns the metadata for a table.
     *
     * @param <K> the row key type
     * @param <C> the column type
     * @param tableName the table name
     * @return the table metadata
     */
    <K, C> TableMetadata<K, C> getTableMetadata(TableName tableName);

    /**
     * Compacts a table.
     *
     * @param tableName the table name
     */
    void compactTable(TableName tableName);

    /**
     * Major compacts a table.
     *
     * @param tableName the table name
     */
    void majorCompactTable(TableName tableName);

    /**
     * Creates a connection to the column store.
     *
     * @return a connection
     */
    Connection createConnection();

    /**
     * Creates a connection to the column store.
     *
     * @param pool the thread pool to use for batch operations
     * @return a connection
     */
    Connection createConnection(ExecutorService pool);

    /**
     * Creates a health check to the column store.
     *
     * @param tableName table name for health check
     * @return a health check
     */
    HealthCheck createHealthCheck(TableName tableName);
}
