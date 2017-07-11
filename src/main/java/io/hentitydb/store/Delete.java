package io.hentitydb.store;

public interface Delete<K, C> extends RowMutation<K, C> {

    /**
     * Deletes all columns for this row.
     *
     * @return the delete operation
     */
    Delete<K, C> addAll();

    /**
     * Deletes a column for this row.
     *
     * @param column the column to delete
     * @return the delete operation
     */
    Delete<K, C> addColumn(C column);

    /**
     * Deletes a column for this row.
     *
     * @param family the column family
     * @param column the column to delete
     * @return the delete operation
     */
    Delete<K, C> addColumn(String family, C column);

    /**
     * Deletes all columns for a family for this row.
     *
     * @param family the column family
     * @return the delete operation
     */
    Delete<K, C> addFamily(String family);

    /**
     * Returns whether columns have been specified for deletion.
     *
     * @return true if columns have been specified for deletion; otherwise false.
     */
    boolean hasColumns();
}
