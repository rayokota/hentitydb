package io.hentitydb.entity;

import io.hentitydb.serialization.ByteBufferCodec;
import io.hentitydb.serialization.BytesUtil;
import io.hentitydb.serialization.ReadBuffer;
import io.hentitydb.serialization.WriteBuffer;
import io.hentitydb.store.CompareOp;

import java.nio.ByteBuffer;
import java.util.Map;

public class ColumnPredicate extends QueryPredicate {
    private final static ByteBufferCodec BYTE_BUFFER_CODEC = new ByteBufferCodec(true);

    private String name;
    private CompareOp op;
    private ByteBuffer value;

    public String getName() {
        return name;
    }

    public CompareOp getOp() {
        return op;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public boolean isAbsentCheck() {
        // note that op is ignored
        return value == null;
    }

    public ColumnPredicate setName(String name) {
        this.name = name;
        return this;
    }

    public ColumnPredicate setOp(CompareOp op) {
        this.op = op;
        return this;
    }

    public ColumnPredicate setValue(ByteBuffer value) {
        this.value = value;
        return this;
    }

    public ColumnPredicate setValue(byte[] value) {
        this.value = ByteBuffer.wrap(value);
        return this;
    }

    public boolean evaluate(Map<String, ByteBuffer> entity) {
        ByteBuffer columnValue = entity.get(getName());
        ByteBuffer predicateValue = getValue();

        // first test if absent check
        if (predicateValue == null) {
            return columnValue == null;
        }

        if (columnValue == null) {
            columnValue = ByteBuffer.allocate(0);
        }
        int cmp = BytesUtil.compareTo(columnValue, predicateValue);

        switch (getOp()) {
            case LESS_THAN:
                return cmp < 0;
            case GREATER_THAN_EQUAL:
                return cmp >= 0;
            case EQUAL:
                return cmp == 0;
            case GREATER_THAN:
                return cmp > 0;
            case LESS_THAN_EQUAL:
                return cmp <= 0;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public void encode(WriteBuffer buffer) {
        buffer.writeUtf8String(getName());
        buffer.writeByte(!isAbsentCheck() ? 1 : 0);
        if (!isAbsentCheck()) {
            buffer.writeByte(getOp().toByte());
            BYTE_BUFFER_CODEC.encode(getValue(), buffer);
        }
    }

    @Override
    public ColumnPredicate decode(ReadBuffer buffer) {
        ColumnPredicate predicate = new ColumnPredicate()
                .setName(buffer.readUtf8String());
        if (buffer.readByte() == 1) {
            predicate.setOp(CompareOp.fromByte(buffer.readByte()))
                    .setValue(BYTE_BUFFER_CODEC.decode(buffer));
        }
        return predicate;
    }

    @Override
    public String toString() {
        return "ColumnPredicate [name=" + name + ", op=" + op + ", value="
                + value + "]";
    }
}
