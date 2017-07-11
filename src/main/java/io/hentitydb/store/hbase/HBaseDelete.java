package io.hentitydb.store.hbase;

import io.hentitydb.serialization.Codec;
import io.hentitydb.store.CompareOp;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;

public class HBaseDelete<K, C> extends HBaseRowMutation<K, C> implements io.hentitydb.store.Delete<K, C> {

    private final String defaultFamily;
    private final Delete delete;
    private final Codec<C> columnCodec;
    private boolean hasColumns;

    public HBaseDelete(K key, String defaultFamily, Codec<K> keyCodec, Codec<C> columnCodec) {
        super(key, null);
        this.defaultFamily = defaultFamily;
        this.columnCodec = columnCodec;
        this.delete = new Delete(HBaseUtil.keyToBytes(key, keyCodec));
        this.hasColumns = false;
    }

    public HBaseDelete(K key, HBaseTable<K, C> table) {
        super(key, table);
        this.defaultFamily = table.getMetadata().getDefaultFamily();
        this.columnCodec = table.getMetadata().getColumnCodec();
        this.delete = new Delete(HBaseUtil.keyToBytes(key, table.getMetadata()));
        this.hasColumns = false;
    }

    public HBaseDelete(byte[] rawKey, String defaultFamily, Codec<C> columnCodec) {
        super(null, null);
        this.defaultFamily = defaultFamily;
        this.columnCodec = columnCodec;
        this.delete = new Delete(rawKey);
        this.hasColumns = false;
    }

    @Override
    public Delete getHOperation() {
        return delete;
    }

    @Override
    public HBaseDelete<K, C> addAll() {
        return this;
    }

    @Override
    public HBaseDelete<K, C> addColumn(C column) {
        return addColumn(defaultFamily, column);
    }

    @Override
    public HBaseDelete<K, C> addColumn(String family, C column) {
        // delete all versions of this column
        delete.addColumns(Bytes.toBytes(family), columnCodec.encode(column));
        hasColumns = true;
        return this;
    }

    @Override
    public HBaseDelete<K, C> addFamily(String family) {
        delete.addFamily(Bytes.toBytes(family));
        return this;
    }

    @Override
    public boolean hasColumns() {
        return hasColumns;
    }

    @Override
    public void execute() {
        getTable().doDelete(this);
    }

    @Override
    public boolean executeIfAbsent(C column) {
        return executeIfAbsent(defaultFamily, column);
    }

    @Override
    public boolean executeIfAbsent(String family, C column) {
        return getTable().doDeleteIf(family, column, CompareOp.EQUAL, null, this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, boolean value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, boolean value) {
        return getTable().doDeleteIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, short value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, short value) {
        return getTable().doDeleteIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, int value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, int value) {
        return getTable().doDeleteIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, long value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, long value) {
        return getTable().doDeleteIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, Date value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, Date value) {
        return getTable().doDeleteIf(family, column, compareOp, Bytes.toBytes(value.getTime()), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, float value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, float value) {
        return getTable().doDeleteIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, double value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, double value) {
        return getTable().doDeleteIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, byte[] value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, byte[] value) {
        return getTable().doDeleteIf(family, column, compareOp, value, this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, String value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, String value) {
        return getTable().doDeleteIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public <V> boolean executeIf(C column, CompareOp compareOp, V value, Codec<V> valueCodec) {
        return executeIf(defaultFamily, column, compareOp, value, valueCodec);
    }

    @Override
    public <V> boolean executeIf(String family, C column, CompareOp compareOp, V value, Codec<V> valueCodec) {
        return getTable().doDeleteIf(family, column, compareOp, valueCodec.encode(value), this);
    }

    @Override
    public HBaseDelete<K, C> setTTL(int ttl) {
        delete.setTTL(ttl);
        return this;
    }
}
