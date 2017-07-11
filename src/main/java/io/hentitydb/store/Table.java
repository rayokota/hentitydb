package io.hentitydb.store;

import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.concurrent.ExecutorService;

public interface Table<K, C> extends AutoCloseable {

    /**
     * Returns the table metadata.
     */
    TableMetadata<K, C> getMetadata();

    /**
     * Returns the connection.
     */
    Connection getConnection();

    /**
     * Creates an executable get operation.
     */
    Get<K, C> get(K key);

    /**
     * Gets all rows.
     *
     * Not recommended for production use.
     */
    @VisibleForTesting
    RowScanner<K, C> getAll();

    /**
     * Creates an executable increment operation.
     */
    Increment<K, C> increment(K key);

    /**
     * Creates an executable put operation.
     */
    Put<K, C> put(K key);

    /**
     * Creates an executable delete operation.
     */
    Delete<K, C> delete(K key);

    /**
     * Creates an executable batch mutation.
     */
    BatchMutation<K, C> batchMutations();

    /**
     * Creates an executable batch operation.
     */
    BatchOperation<K, C> batchOperations();

    /**
     * Creates an executable list of atomic mutation operations.
     */
    RowMutations<K, C> mutateRow(K key);

    /**
     * Scans over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @return a {@link RowScanner} for the given range
     */
    RowScanner<K, C> scan(K startKey,
                          K endKey);

    /**
     * Scans over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @param filter      a row filter to apply
     * @return a {@link RowScanner} for the given range
     */
    RowScanner<K, C> scan(K startKey,
                          K endKey,
                          Filter<K, C> filter);

    /**
     * Scans over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @param executor    an executor for parallel scans, if supported
     * @return a {@link RowScanner} for the given range
     */
    RowScanner<K, C> scan(K startKey,
                          K endKey,
                          ExecutorService executor);

    /**
     * Scans over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @param filter      a row filter to apply
     * @param executor    an executor for parallel scans, if supported
     * @return a {@link RowScanner} for the given range
     */
    RowScanner<K, C> scan(K startKey,
                          K endKey,
                          Filter<K, C> filter,
                          ExecutorService executor);

    /**
     * Scans in reverse over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @return a {@link RowScanner} for the given range
     */
    RowScanner<K, C> reverseScan(K startKey,
                                 K endKey);

    /**
     * Scans in reverse over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @param filter      a row filter to apply
     * @return a {@link RowScanner} for the given range
     */
    RowScanner<K, C> reverseScan(K startKey,
                                 K endKey,
                                 Filter<K, C> filter);

    /**
     * Scans in reverse over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @param executor    an executor for parallel scans, if supported
     * @return a {@link RowScanner} for the given range
     */
    RowScanner<K, C> reverseScan(K startKey,
                                 K endKey,
                                 ExecutorService executor);

    /**
     * Scans in reverse over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @param filter      a row filter to apply
     * @param executor    an executor for parallel scans, if supported
     * @return a {@link RowScanner} for the given range
     */
    RowScanner<K, C> reverseScan(K startKey,
                                 K endKey,
                                 Filter<K, C> filter,
                                 ExecutorService executor);

    /**
     * Truncates the table.
     *
     * Not recommended for production use.
     */
    @VisibleForTesting
    void truncate();

    /**
     * Returns a raw representation of the filter.  For internal use only.
     */
    byte[] getRawFilters(List<Filter<K, C>> filters);
}
