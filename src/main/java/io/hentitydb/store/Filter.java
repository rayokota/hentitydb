package io.hentitydb.store;

import io.hentitydb.serialization.Codec;

import java.util.List;
import java.util.Set;

public interface Filter<K, C> extends Codec<Filter<K, C>> {

    /**
     * Rest the filter state before filtering a new row.
     */
    void reset();

    /**
     * If this returns true, skip to the next row.
     *
     * @return true to skip to the next row; otherwise false
     */
    boolean ignoreRemainingRow();

    /**
     * Determines whether the value should be included in the result.
     *
     * @param keyColumn the key column
     * @return true if the value should be included; otherwise false
     */
    boolean filterKeyColumn(KeyColumn<K, C> keyColumn);

    /**
     * Gives the filter a chance to transform the value.
     *
     * @param keyColumn the key column
     * @return the transformed value, or null if the old value should be used
     */
    byte[] transformKeyColumn(KeyColumn<K, C> keyColumn);

    /**
     * Whether the filterRow method should be called.
     *
     * @return true if the filterRow method should be called
     */
    boolean hasFilterRow();

    /**
     * Determines which columns for a row should be kept.
     *
     * @param columns the key columns
     * @return the indexes of the key columns to keep
     */
    Set<Integer> filterRow(List<KeyColumn<K, C>> columns);
}
