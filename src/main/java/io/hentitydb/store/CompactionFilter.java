package io.hentitydb.store;

import java.util.Map;

public interface CompactionFilter<K, C> {

    String HENTITYDB_PREFIX = "hentitydb";
    String FILTER = "filter";
    String KEY_CODEC = "keyCodec";
    String SALTED_KEY_CODEC = "saltedKeyCodec";
    String COLUMN_CODEC = "columnCodec";

    /**
     * Sets up the compaction filter.
     *
     * @param config the configuration
     * @param tableName the table name
     * @param family the column family name
     * @param regionName the region name
     */
    void setup(Map<String, String> config, TableName tableName, String family, String regionName);

    /**
     * Determines whether the value should be kept during compaction.
     *
     * @param keyColumn the key column
     * @return true if the value should be kept; otherwise false
     */
    boolean filterKeyColumn(KeyColumn<K, C> keyColumn);

    /**
     * Returns a Delete operation to delete auxiliary columns for the filter which are from the same row.
     *
     * @param filter the result of filterKeyColumn
     * @param keyColumn the key column
     * @param delete a Delete operation for this row to which columns should be added
     * @return the modified Delete operation, or null
     */
    Delete<K, C> deleteAuxiliaryColumns(Delete<K, C> delete, boolean filter, KeyColumn<K, C> keyColumn);

    /**
     * Closes the compaction filter.
     */
    void close();

}
