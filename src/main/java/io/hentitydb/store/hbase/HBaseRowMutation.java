package io.hentitydb.store.hbase;

import io.hentitydb.store.RowMutation;

public abstract class HBaseRowMutation<K, C> extends HBaseRowOperation<K, C> implements RowMutation<K, C> {

    public HBaseRowMutation(K key, HBaseTable<K, C> table) {
        super(key, table);
    }
}
