package io.hentitydb.store;

import io.hentitydb.serialization.Codec;

import java.util.List;
import java.util.Map;

public class ColumnFamilyMetadata<K, C> {

    private final String name;
    private final Integer maxColumns;
    private final Integer maxColumnsTtl;
    private final String referencingFamily;
    private final String indexingFamily;
    private final List<Codec<?>> indexingCodecs;
    private final Integer ttl;
    private final Class<? extends CompactionFilter<K, C>> compactionFilter;
    private final Map<String, String> compactionFilterProps;

    /**
     * Creates a column family metadata object.
     *
     * @param name the name of the column family
     * @param maxColumns the maximum number of columns
     * @param ttl the time-to-live in seconds of each column value
     */
    public ColumnFamilyMetadata(String name,
                                Integer maxColumns,
                                Integer ttl) {
        this.name = name;
        this.maxColumns = maxColumns;
        this.maxColumnsTtl = null;
        this.referencingFamily = null;
        this.indexingFamily = null;
        this.indexingCodecs = null;
        this.ttl = ttl;
        this.compactionFilter = null;
        this.compactionFilterProps = null;
    }

    /**
     * Creates a column family metadata object.
     *
     * @param name the name of the column family
     * @param maxColumns the maximum number of columns
     * @param maxColumnsTtl the time-to-live in seconds for a column if the maximum number of columns are present,
     *                      if the ttl has not been met, the column is not removed even if the maximum number exist
     * @param ttl the time-to-live in seconds of each column value
     */
    public ColumnFamilyMetadata(String name,
                                Integer maxColumns,
                                Integer maxColumnsTtl,
                                Integer ttl) {
        this.name = name;
        this.maxColumns = maxColumns;
        this.maxColumnsTtl = maxColumnsTtl;
        this.referencingFamily = null;
        this.indexingFamily = null;
        this.indexingCodecs = null;
        this.ttl = ttl;
        this.compactionFilter = null;
        this.compactionFilterProps = null;
    }

    /**
     * Creates a column family metadata object.
     *
     * @param name the name of the column family
     * @param maxColumns the maximum number of columns
     * @param maxColumnsTtl the time-to-live in seconds for a column if the maximum number of columns are present,
     *                      if the ttl has not been met, the column is not removed even if the maximum number exist
     * @param referencingFamily another column family in the same row which references this column family
     * @param indexingFamily another column family in the same row which indexes this column family
     * @param ttl the time-to-live in seconds of each column value
     */
    public ColumnFamilyMetadata(String name,
                                Integer maxColumns,
                                Integer maxColumnsTtl,
                                String referencingFamily,
                                String indexingFamily,
                                List<Codec<?>> indexingCodecs,
                                Integer ttl) {
        this.name = name;
        this.maxColumns = maxColumns;
        this.maxColumnsTtl = maxColumnsTtl;
        this.referencingFamily = referencingFamily;
        this.indexingFamily = indexingFamily;
        this.indexingCodecs = indexingCodecs;
        this.ttl = ttl;
        this.compactionFilter = null;
        this.compactionFilterProps = null;
    }

    /**
     * Creates a column family metadata object.
     *
     * @param name the name of the column family
     * @param ttl the time-to-live in seconds of each column value
     * @param compactionFilter a filter to be used during compaction
     * @param compactionFilterProps properties for use by the compaction filter
     */
    public ColumnFamilyMetadata(String name,
                                Integer ttl,
                                Class<? extends CompactionFilter<K, C>> compactionFilter,
                                Map<String, String> compactionFilterProps) {
        this.name = name;
        this.maxColumns = null;
        this.maxColumnsTtl = null;
        this.referencingFamily = null;
        this.indexingFamily = null;
        this.indexingCodecs = null;
        this.ttl = ttl;
        this.compactionFilter = compactionFilter;
        this.compactionFilterProps = compactionFilterProps;
    }

    /**
     * Returns the column family name.
     *
     * @return the column family name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the maximum number of columns.
     *
     * @return the maximum number of columns, or null if none
     */
    public Integer getMaxColumns() {
        return maxColumns;
    }

    /**
     * Returns the time-to-live in seconds for a column if the maximum number of columns are present,
     * if the ttl has not been met, the column is not removed even if the maximum number exist.
     *
     * @return the ttl, or null if none
     */
    public Integer getMaxColumnsTtl() {
        return maxColumnsTtl;
    }

    /**
     * Returns another column family in the same row which references this column family.
     *
     * It is assumed that columns in the referencing family refer to columns with the same name
     * in this family.
     *
     * @return the referencing family, or null if none
     */
    public String getReferencingFamily() {
        return referencingFamily;
    }

    /**
     * Returns another column family in the same row which indexes this column family.
     *
     * @return the indexing family, or null if none
     */
    public String getIndexingFamily() {
        return indexingFamily;
    }

    /**
     * Returns a list of codecs, the last of which is used for decoding the index value.
     *
     * @return the list of codecs
     */
    public List<Codec<?>> getIndexingCodecs() {
        return indexingCodecs;
    }

    /**
     * Returns the time-to-live in seconds for each column value.
     *
     * @return the ttl, or null if none
     */
    public Integer getTimeToLive() {
        return ttl;
    }

    /**
     * Returns the compaction filter to be used if maxColumns is not specified.
     *
     * @return the compaction filter, or none.
     */
    public Class<? extends CompactionFilter<K, C>> getCompactionFilter() {
        return compactionFilter;
    }

    /**
     * Returns the compaction filter properties.
     *
     * @return the compaction filter properties, or none.
     */
    public Map<String, String> getCompactionFilterProps() {
        return compactionFilterProps;
    }
}
