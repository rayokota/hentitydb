package io.hentitydb.store;

import io.hentitydb.serialization.Codec;

import java.util.Date;

public class TestColumn<C, V> implements Column<C> {

    private final C name;
    private final V value;
    private final Codec<V> valueCodec;
    private final long timestamp;

    public TestColumn(C name, V value) {
        this.name = name != null ? name : null;
        this.value = value != null ? value : null;
        this.valueCodec = null;
        this.timestamp = System.currentTimeMillis();
    }

    public TestColumn(C name, V value, long timestamp) {
        this.name = name != null ? name : null;
        this.value = value != null ? value : null;
        this.valueCodec = null;
        this.timestamp = timestamp;
    }

    public TestColumn(C name, V value, Codec<V> valueCodec, long timestamp) {
        this.name = name != null ? name : null;
        this.value = value != null ? value : null;
        this.valueCodec = valueCodec;
        this.timestamp = timestamp;
    }

    @Override
    public Codec<C> getColumnCodec() {
        return null;
    }

    @Override
    public C getName() {
        return name != null ? name : null;
    }

    @Override
    public byte[] getRawName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getFamily() {
        return "cf";
    }

    @Override
    public boolean getBoolean() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes() {
        return valueCodec.encode(value);
    }

    @Override
    public String getString() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> V getValue(Codec<V> valueCodec) {
        return (V) value;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }
}
