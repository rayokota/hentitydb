package io.hentitydb.entity;

import io.hentitydb.serialization.Codec;
import io.hentitydb.store.Column;

import java.util.Date;

public class TestColumn implements Column<byte[]> {

    private final byte[] name;
    private final byte[] value;
    private final long timestamp;

    public TestColumn(byte[] name, byte[] value) {
        this.name = name != null ? name.clone() : null;
        this.value = value != null ? value.clone() : null;
        this.timestamp = System.currentTimeMillis();
    }

    public TestColumn(byte[] name, byte[] value, long timestamp) {
        this.name = name != null ? name.clone() : null;
        this.value = value != null ? value.clone() : null;
        this.timestamp = timestamp;
    }

    @Override
    public Codec<byte[]> getColumnCodec() {
        return null;
    }

    @Override
    public byte[] getName() {
        return name != null ? name.clone() : null;
    }

    @Override
    public byte[] getRawName() {
        return name != null ? name.clone() : null;
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
        return value != null ? value.clone() : null;
    }

    @Override
    public String getString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> V getValue(Codec<V> valueCodec) {
        return valueCodec.decode(getBytes());
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }
}
