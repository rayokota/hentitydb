package io.hentitydb.store.hbase;

import io.hentitydb.serialization.Codec;
import io.hentitydb.store.CompareOp;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;

public class HBasePut<K, C> extends HBaseRowMutation<K, C> implements io.hentitydb.store.Put<K, C> {

    private final String defaultFamily;
    private final Put put;
    private final Codec<C> columnCodec;

    public HBasePut(K key, String defaultFamily, Codec<K> keyCodec, Codec<C> columnCodec) {
        super(key, null);
        this.defaultFamily = defaultFamily;
        this.columnCodec = columnCodec;
        this.put = new Put(HBaseUtil.keyToBytes(key, keyCodec));
    }

    public HBasePut(K key, HBaseTable<K, C> table) {
        super(key, table);
        this.defaultFamily = table.getMetadata().getDefaultFamily();
        this.columnCodec = table.getMetadata().getColumnCodec();
        this.put = new Put(HBaseUtil.keyToBytes(key, table.getMetadata()));
    }

    public HBasePut(byte[] rawKey, String defaultFamily, Codec<C> columnCodec) {
        super(null, null);
        this.defaultFamily = defaultFamily;
        this.columnCodec = columnCodec;
        this.put = new Put(rawKey);
    }

    @Override
    public Put getHOperation() {
        return put;
    }

    @Override
    public HBasePut<K, C> addColumn(C column, boolean value) {
        return addColumn(defaultFamily, column, value);
    }

    @Override
    public HBasePut<K, C> addColumn(String family, C column, boolean value) {
        put.addImmutable(Bytes.toBytes(family), columnCodec.encode(column), Bytes.toBytes(value));
        return this;
    }

    @Override
    public HBasePut<K, C> addColumn(C column, short value) {
        return addColumn(defaultFamily, column, value);
    }

    @Override
    public HBasePut<K, C> addColumn(String family, C column, short value) {
        put.addImmutable(Bytes.toBytes(family), columnCodec.encode(column), Bytes.toBytes(value));
        return this;
    }

    @Override
    public HBasePut<K, C> addColumn(C column, int value) {
        return addColumn(defaultFamily, column, value);
    }

    @Override
    public HBasePut<K, C> addColumn(String family, C column, int value) {
        put.addImmutable(Bytes.toBytes(family), columnCodec.encode(column), Bytes.toBytes(value));
        return this;
    }

    @Override
    public HBasePut<K, C> addColumn(C column, long value) {
        return addColumn(defaultFamily, column, value);
    }

    @Override
    public HBasePut<K, C> addColumn(String family, C column, long value) {
        put.addImmutable(Bytes.toBytes(family), columnCodec.encode(column), Bytes.toBytes(value));
        return this;
    }

    @Override
    public HBasePut<K, C> addColumn(C column, Date value) {
        return addColumn(defaultFamily, column, value);
    }

    @Override
    public HBasePut<K, C> addColumn(String family, C column, Date value) {
        put.addImmutable(Bytes.toBytes(family), columnCodec.encode(column), Bytes.toBytes(value.getTime()));
        return this;
    }

    @Override
    public HBasePut<K, C> addColumn(C column, float value) {
        return addColumn(defaultFamily, column, value);
    }

    @Override
    public HBasePut<K, C> addColumn(String family, C column, float value) {
        put.addImmutable(Bytes.toBytes(family), columnCodec.encode(column), Bytes.toBytes(value));
        return this;
    }

    @Override
    public HBasePut<K, C> addColumn(C column, double value) {
        return addColumn(defaultFamily, column, value);
    }

    @Override
    public HBasePut<K, C> addColumn(String family, C column, double value) {
        put.addImmutable(Bytes.toBytes(family), columnCodec.encode(column), Bytes.toBytes(value));
        return this;
    }

    @Override
    public HBasePut<K, C> addColumn(C column, byte[] value) {
        return addColumn(defaultFamily, column, value);
    }

    @Override
    public HBasePut<K, C> addColumn(String family, C column, byte[] value) {
        put.addImmutable(Bytes.toBytes(family), columnCodec.encode(column), value);
        return this;
    }

    @Override
    public HBasePut<K, C> addColumn(C column, String value) {
        return addColumn(defaultFamily, column, value);
    }

    @Override
    public HBasePut<K, C> addColumn(String family, C column, String value) {
        put.addImmutable(Bytes.toBytes(family), columnCodec.encode(column), Bytes.toBytes(value));
        return this;
    }

    @Override
    public <V> HBasePut<K, C> addColumn(C column, V value, Codec<V> valueCodec) {
        return addColumn(defaultFamily, column, value, valueCodec);
    }

    @Override
    public <V> HBasePut<K, C> addColumn(String family, C column, V value, Codec<V> valueCodec) {
        put.addImmutable(Bytes.toBytes(family), columnCodec.encode(column), valueCodec.encode(value));
        return this;
    }

    @Override
    public void execute() {
        getTable().doPut(this);
    }

    @Override
    public boolean executeIfAbsent(C column) {
        return executeIfAbsent(defaultFamily, column);
    }

    @Override
    public boolean executeIfAbsent(String family, C column) {
        return getTable().doPutIf(family, column, CompareOp.EQUAL, null, this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, boolean value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, boolean value) {
        return getTable().doPutIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, short value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, short value) {
        return getTable().doPutIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, int value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, int value) {
        return getTable().doPutIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, long value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, long value) {
        return getTable().doPutIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, Date value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, Date value) {
        return getTable().doPutIf(family, column, compareOp, Bytes.toBytes(value.getTime()), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, float value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, float value) {
        return getTable().doPutIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, double value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, double value) {
        return getTable().doPutIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, byte[] value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, byte[] value) {
        return getTable().doPutIf(family, column, compareOp, value, this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, String value) {
        return executeIf(defaultFamily, column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, String value) {
        return getTable().doPutIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public <V> boolean executeIf(C column, CompareOp compareOp, V value, Codec<V> valueCodec) {
        return executeIf(defaultFamily, column, compareOp, value, valueCodec);
    }

    @Override
    public <V> boolean executeIf(String family, C column, CompareOp compareOp, V value, Codec<V> valueCodec) {
        return getTable().doPutIf(family, column, compareOp, valueCodec.encode(value), this);
    }

    @Override
    public HBasePut<K, C> setTTL(int ttl) {
        put.setTTL(ttl);
        return this;
    }
}
