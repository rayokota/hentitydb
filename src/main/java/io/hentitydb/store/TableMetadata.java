package io.hentitydb.store;

import com.google.common.collect.Lists;
import io.hentitydb.serialization.Codec;
import io.hentitydb.serialization.SaltingCodec;

import java.util.List;
import java.util.Map;

public class TableMetadata<K, C> {

    private final TableName tableName;
    private final String defaultFamily;
    private final List<ColumnFamilyMetadata<K, C>> families;
    private final Codec<K> keyCodec;
    private final Codec<C> columnCodec;

    /**
     * Creates a table metadata object.
     *
     * @param tableName the name of the table
     * @param defaultFamily the default column family name
     * @param keyCodec the codec for the key
     * @param columnCodec the codec for the column names
     */
    public TableMetadata(TableName tableName,
                         String defaultFamily,
                         Codec<K> keyCodec,
                         Codec<C> columnCodec) {
        this.tableName = tableName;
        this.defaultFamily = defaultFamily;
        this.families = Lists.newArrayList();
        this.families.add(new ColumnFamilyMetadata<>(defaultFamily, null, null));
        this.keyCodec = keyCodec;
        this.columnCodec = columnCodec;
    }

    /**
     * Creates a table metadata object.
     *
     * @param tableName the name of the table
     * @param defaultFamily the default column family name
     * @param keyCodec the codec for the key
     * @param columnCodec the codec for the column names
     * @param maxColumns the default maximum number of columns for each column family
     * @param ttl the default time-to-live in seconds of each column value in each column family
     * @param options a map of options for table creation
     */
    public TableMetadata(TableName tableName,
                         String defaultFamily,
                         Codec<K> keyCodec,
                         Codec<C> columnCodec,
                         Integer maxColumns,
                         Integer ttl,
                         Map<String, Object> options) {
        this.tableName = tableName;
        this.defaultFamily = defaultFamily;
        this.families = Lists.newArrayList();
        this.families.add(new ColumnFamilyMetadata<>(defaultFamily, maxColumns, ttl));
        this.keyCodec = keyCodec;
        this.columnCodec = columnCodec;
    }

    /**
     * Creates a table metadata object.
     *
     * @param tableName the name of the table
     * @param defaultFamily the default column family name
     * @param keyCodec the codec for the key
     * @param columnCodec the codec for the column names
     * @param maxColumns the default maximum number of columns for each column family
     * @param referencingFamily another column family in the same row which references this column family
     * @param indexingFamily another column family in the same row which indexes this column family
     * @param indexingCodecs a list of codecs, the last of which is used for decoding the index value
     * @param ttl the default time-to-live in seconds of each column value in each column family
     */
    public TableMetadata(TableName tableName,
                         String defaultFamily,
                         Codec<K> keyCodec,
                         Codec<C> columnCodec,
                         Integer maxColumns,
                         String referencingFamily,
                         String indexingFamily,
                         List<Codec<?>> indexingCodecs,
                         Integer ttl) {
        this.tableName = tableName;
        this.defaultFamily = defaultFamily;
        this.families = Lists.newArrayList();
        this.families.add(new ColumnFamilyMetadata<>(
                defaultFamily, maxColumns, null, referencingFamily, indexingFamily, indexingCodecs, ttl));
        this.keyCodec = keyCodec;
        this.columnCodec = columnCodec;
    }

    /**
     * Creates a table metadata object.
     *
     * @param tableName the name of the table
     * @param defaultFamily the default column family
     * @param keyCodec the codec for the key
     * @param columnCodec the codec for the column names
     */
    public TableMetadata(TableName tableName,
                         ColumnFamilyMetadata<K, C> defaultFamily,
                         Codec<K> keyCodec,
                         Codec<C> columnCodec) {
        this.tableName = tableName;
        this.defaultFamily = defaultFamily.getName();
        this.families = Lists.newArrayList();
        this.families.add(defaultFamily);
        this.keyCodec = keyCodec;
        this.columnCodec = columnCodec;
    }

    /**
     * Returns the table name.
     *
     * @return the table name
     */
    public TableName getTableName() {
        return tableName;
    }

    /**
     * Returns the default column family.
     *
     * @return the default column family
     */
    public String getDefaultFamily() {
        return defaultFamily;
    }

    /**
     * Adds a column family.
     *
     * @param family the column family
     * @return the table metadata
     */
    public TableMetadata<K, C> addColumnFamily(ColumnFamilyMetadata<K, C> family) {
        families.add(family);
        return this;
    }

    /**
     * Returns the column families, including the default.
     *
     * @return the column families
     */
    public List<ColumnFamilyMetadata<K, C>> getColumnFamilies() {
        return families;
    }

    /**
     * Returns the column family names, including the default.
     *
     * @return the column family names
     */
    public List<String> getColumnFamilyNames() {
        List<String> columnFamilies = Lists.newArrayListWithExpectedSize(families.size());
        for (ColumnFamilyMetadata family : families) {
            columnFamilies.add(family.getName());
        }
        return columnFamilies;
    }

    /**
     * Returns the codec for the key.
     *
     * @return the key codec
     */
    public Codec<K> getKeyCodec() {
        return keyCodec;
    }

    /**
     * Returns the codec for the column names.
     *
     * @return the column name codec
     */
    public Codec<C> getColumnCodec() {
        return columnCodec;
    }

    /**
     * Returns whether the table is salted.
     *
     * @return true if the table is salted; otherwise false
     */
    public boolean isSalted() {
        return keyCodec instanceof SaltingCodec<?>;
    }

    /**
     * Returns the possible salting prefixes.
     *
     * @return an array of salting prefixes if the table is salted; otherwise an empty array.
     */
    public byte[][] getAllSaltingPrefixes() {
        return isSalted() ? ((SaltingCodec<K>)keyCodec).getAllSaltingPrefixes() : new byte[0][0];
    }
}
