package io.hentitydb.store;

import java.util.List;

public interface BatchOperation<K, C> {

    /**
     * Adds a row operation to the batch.
     *
     * @param rowOperation a row operation
     * @return the batch operation
     */
    BatchOperation<K, C> add(RowOperation<K, C> rowOperation);

    /**
     * Adds a list of row operations to the batch.
     *
     * @param rowOperations a List of row operations
     * @return the batch operation
     */
    BatchOperation<K, C> add(List<RowOperation<K, C>> rowOperations);

    /**
     * Executes a batch of row operations.
     *
     * Each result will either be null if the operation failed to communicate with the server, an empty row for
     * put and delete operations, a valid row for get operations, or an exception returned by the server.
     *
     * @return the results of the operations
     *
     */
    Object[] execute();
}
