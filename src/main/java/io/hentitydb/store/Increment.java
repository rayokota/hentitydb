package io.hentitydb.store;

public interface Increment<K, C> extends RowMutation<K, C> {

    /**
     * Specifies a counter to be incremented.
     *
     * @param column the column name
     * @param amount the increment amount
     * @return the increment operation
     */
    Increment<K, C> addColumn(C column, long amount);

    /**
     * Specifies a counter to be incremented.
     *
     * @param family the column family
     * @param column the column name
     * @param amount the increment amount
     * @return the increment operation
     */
    Increment<K, C> addColumn(String family, C column, long amount);
}
