package io.hentitydb.store.hbase;

import io.hentitydb.store.RowOperation;
import org.apache.hadoop.hbase.client.Row;

public abstract class HBaseRowOperation<K, C> implements RowOperation<K, C> {

    private final K key;
    private final HBaseTable<K, C> table;

    public HBaseRowOperation(K key, HBaseTable<K, C> table) {
        this.key = key;
        this.table = table;
    }

    public K getKey() {
        return key;
    }

    public HBaseTable<K, C> getTable() {
        return table;
    }

    public abstract Row getHOperation();
}
