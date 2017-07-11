package io.hentitydb.store;

import java.util.List;

public interface BatchMutation<K, C> {

    /**
     * Adds a row mutation to the batch.
     *
     * @param rowMutation a row mutation
     * @return the batch mutation
     */
    BatchMutation<K, C> add(RowMutation<K, C> rowMutation);

    /**
     * Adds a list of row mutations to the batch.
     *
     * @param rowMutation a row mutation
     * @return the batch mutation
     */
    BatchMutation<K, C> add(List<RowMutation<K, C>> rowMutation);

    /**
     * Executes a batch of row mutations.
     */
    void execute();
}
