package io.hentitydb.store;

public interface Connection extends AutoCloseable {

    /**
     * Returns the connection factory.
     *
     * @return the connection factory
     */
    ConnectionFactory getConnectionFactory();

    /**
     * Returns the table with the given name.
     *
     * @param <K> the row key type
     * @param <C> the column type
     * @param tableName the table name
     * @return the table
     *
     */
    <K, C> Table<K, C> getTable(TableName tableName);
}
