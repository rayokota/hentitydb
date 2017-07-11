package io.hentitydb.entity;

import io.hentitydb.serialization.Codec;

import javax.persistence.Column;
import javax.persistence.OrderBy;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;

public class FieldMapper<T> {
    final Codec<T> serializer;
    final Field field;
    final String name;
    final boolean reversed;

    enum Order {
        ASC,
        DESC,
    }

    public FieldMapper(final Field field) {
        this.field = field;

        Column columnAnnotation = field.getAnnotation(Column.class);
        if (columnAnnotation == null || columnAnnotation.name().isEmpty()) {
            name = field.getName();
        } else {
            name = columnAnnotation.name();
        }

        OrderBy orderByAnnotation = field.getAnnotation(OrderBy.class);
        if (orderByAnnotation == null) {
            reversed = false;
        } else {
            Class<?> cls = field.getType();
            if (!cls.equals(Long.class) && !cls.equals(long.class) &&
                !cls.equals(Integer.class) && !cls.equals(int.class)) {
                throw new IllegalArgumentException("@OrderBy annotation only supported on int and long fields");
            }
            Order order = Order.valueOf(orderByAnnotation.value());
            reversed = (order == Order.DESC);
        }

        this.serializer = MappingUtils.getCodecForField(field, reversed);
    }

    public Field getField() {
        return field;
    }

    public Codec<T> getCodec() {
        return serializer;
    }

    public ByteBuffer toByteBuffer(Object entity) throws IllegalArgumentException, IllegalAccessException {
        return valueToByteBuffer(getValue(entity));
    }

    public ByteBuffer toByteBuffer(Map<String, Object> entity) throws IllegalArgumentException {
        return valueToByteBuffer(entity.get(field.getName()));
    }

    public T fromByteBuffer(ByteBuffer buffer) {
        if (buffer.remaining() == 0) return null;
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return serializer.decode(bytes);
    }

    @SuppressWarnings("unchecked")
    public T getValue(Object entity) throws IllegalArgumentException, IllegalAccessException {
        return (T) field.get(entity);
    }

    @SuppressWarnings("unchecked")
    public ByteBuffer valueToByteBuffer(Object value) {
        return value != null ? ByteBuffer.wrap(serializer.encode((T) value)) : null;
    }

    public void setValue(Object entity, Object value) throws IllegalArgumentException, IllegalAccessException {
        field.set(entity, value);
    }

    public void setField(Object entity, ByteBuffer buffer) throws IllegalArgumentException, IllegalAccessException {
        field.set(entity, fromByteBuffer(buffer));
    }

    public boolean isAscending() {
        return !reversed;
    }

    public boolean isDescending() {
        return reversed;
    }

    public String getName() {
        return name;
    }
}
