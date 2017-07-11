package io.hentitydb.store.hbase;

import io.hentitydb.serialization.Codec;
import io.hentitydb.store.CompareOp;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;

public class HBaseIncrement<K, C> extends HBaseRowMutation<K, C> implements io.hentitydb.store.Increment<K, C> {

    private final String defaultFamily;
    private final Increment increment;
    private final Codec<C> columnCodec;

    public HBaseIncrement(K key, String defaultFamily, Codec<K> keyCodec, Codec<C> columnCodec) {
        super(key, null);
        this.defaultFamily = defaultFamily;
        this.columnCodec = columnCodec;
        this.increment = new Increment(HBaseUtil.keyToBytes(key, keyCodec));
    }

    public HBaseIncrement(K key, HBaseTable<K, C> table) {
        super(key, table);
        this.defaultFamily = table.getMetadata().getDefaultFamily();
        this.columnCodec = table.getMetadata().getColumnCodec();
        this.increment = new Increment(HBaseUtil.keyToBytes(key, table.getMetadata()));
    }

    public HBaseIncrement(byte[] rawKey, String defaultFamily, Codec<C> columnCodec) {
        super(null, null);
        this.defaultFamily = defaultFamily;
        this.columnCodec = columnCodec;
        this.increment = new Increment(rawKey);
    }

    @Override
    public Increment getHOperation() {
        return increment;
    }

    @Override
    public HBaseIncrement<K, C> addColumn(C column, long amount) {
        return addColumn(defaultFamily, column, amount);
    }

    @Override
    public HBaseIncrement<K, C> addColumn(String family, C column, long amount) {
        increment.addColumn(Bytes.toBytes(family), columnCodec.encode(column), amount);
        return this;
    }

    @Override
    public void execute() {
        getTable().doIncrement(this);
    }

    @Override
    public boolean executeIfAbsent(C column) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIfAbsent(String family, C column) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, boolean value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, boolean value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, short value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, short value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, Date value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, Date value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, double value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, double value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> boolean executeIf(C column, CompareOp compareOp, V value, Codec<V> valueCodec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> boolean executeIf(String family, C column, CompareOp compareOp, V value, Codec<V> valueCodec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public HBaseIncrement<K, C> setTTL(int ttl) {
        increment.setTTL(ttl);
        return this;
    }
}
