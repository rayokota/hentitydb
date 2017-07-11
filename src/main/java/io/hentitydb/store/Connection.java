package io.hentitydb.store;

public interface Connection extends AutoCloseable {

    /**
     * Returns the connection factory.
     */
    ConnectionFactory getConnectionFactory();

    /**
     * Returns the table with the given name.
     *
     * @param tableName the table name
     */
    <K, C> Table<K, C> getTable(TableName tableName);
}
