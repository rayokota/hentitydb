package io.hentitydb.store;

public interface RowMutations<K, C> extends RowMutation<K, C> {

    /**
     * Adds a row mutation to the list of atomic row mutations for a given row.
     *
     * @param rowMutation a row mutation
     * @return the atomic row mutations
     */
    RowMutations<K, C> add(RowMutation<K, C>... rowMutation);

    /**
     * Executes a list of atomic row mutations for the same row in the given order.
     */
    void execute();
}
