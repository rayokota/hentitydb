package io.hentitydb.store;

public interface Get<K, C> extends RowOperation<K, C> {

    /**
     * Specifies all columns for retrieval.
     *
     * @return the get operation
     */
    Get<K, C> addAll();

    /**
     * Specifies a column for retrieval.
     *
     * @param column the column to retrieve
     * @return the get operation
     */
    Get<K, C> addColumn(C column);

    /**
     * Specifies a column for retrieval.
     *
     * @param family the column family
     * @param column the column to retrieve
     * @return the get operation
     */
    Get<K, C> addColumn(String family, C column);

    /**
     * Specifies a family for retrieval.
     *
     * @param family the column family
     * @return the get operation
     */
    Get<K, C> addFamily(String family);

    /**
     * Specifies the maximum number of columns to retrieve.
     *
     * @param limit the maximum number of columns to retrieve
     * @return the get operation
     */
    Get<K, C> withColumnLimit(int limit);

    /**
     * Specifies a range of columns to retrieve.
     *
     * @param startColumn the start of the range
     * @param endColumn the end of the range
     * @return the get operation
     */
    Get<K, C> withColumnRange(C startColumn, C endColumn);

    /**
     * Specifies a range of columns to retrieve.
     *
     * @param startColumn the start of the range
     * @param endColumn the end of the range
     * @param limit the maximum number of columns to retrieve
     * @return the get operation
     */
    Get<K, C> withColumnRange(C startColumn, C endColumn, int limit);

    /**
     * Specifies a range of columns to retrieve.
     *
     * @param startColumn the start of the range
     * @param startColumnInclusive whether the start column is inclusive
     * @param endColumn the end of the range
     * @param endColumnInclusive whether the start column is inclusive
     * @param limit the maximum number of columns to retrieve
     * @return the get operation
     */
    Get<K, C> withColumnRange(C startColumn,
                              boolean startColumnInclusive,
                              C endColumn,
                              boolean endColumnInclusive,
                              int limit);

    /**
     * Specifies a range of columns to retrieve.
     *
     * @param startColumn the start of the range
     * @param startColumnInclusive whether the start column is inclusive
     * @param endColumn the end of the range
     * @param endColumnInclusive whether the start column is inclusive
     * @param filters a filter that will be applied to the column range
     * @param limit the maximum number of columns to retrieve
     * @return the get operation
     */
    Get<K, C> withColumnRange(C startColumn,
                              boolean startColumnInclusive,
                              C endColumn,
                              boolean endColumnInclusive,
                              int limit,
                              Filter<K, C>... filters);

    /**
     * Adds a column filter.
     *
     * @param filter the column filter to add
     * @return the get operation
     */
    Get<K, C> addFilter(Filter<K, C> filter);

    /**
     * Sets the boolean operator for multiple filters (including the column range).
     *
     * @param op the boolean op for multiple filters
     * @return the get operation
     */
    Get<K, C> setFilterOp(BooleanOp op);

    /**
     * Executes the get operation.
     *
     * @return the resulting row
     */
    Row<K, C> execute();
}
