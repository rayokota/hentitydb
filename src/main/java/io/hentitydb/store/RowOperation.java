package io.hentitydb.store;

public interface RowOperation<K, C> {

    /**
     * Returns the key for the row operation.
     *
     * @return the key
     */
    K getKey();
}
