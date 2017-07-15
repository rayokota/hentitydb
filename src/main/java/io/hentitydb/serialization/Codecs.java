package io.hentitydb.serialization;

import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

/**
 * Utility class that infers the concrete {@link Codec} needed to turn a value into
 * its binary representation.
 */
public class Codecs {

    public static <T> Codec<T> getCodec(Class<?> valueClass) {
        return getCodec(valueClass, false);
    }

    @SuppressWarnings("unchecked")
    public static <T> Codec<T> getCodec(Class<?> valueClass, boolean isDescending) {
        final Codec serializer;
        if (valueClass.equals(String.class)) {
            serializer = new StringCodec();
        }
        else if (valueClass.equals(Long.class) || valueClass.equals(long.class)) {
            serializer = isDescending ? new InvertedLongCodec() : new LongCodec();
        }
        else if (valueClass.equals(Integer.class) || valueClass.equals(int.class)) {
            serializer = isDescending ? new InvertedIntCodec() : new IntCodec();
        }
        else if (valueClass.equals(Short.class) || valueClass.equals(short.class)) {
            serializer = new ShortCodec();
        }
        else if (valueClass.equals(Byte.class) || valueClass.equals(byte.class)) {
            serializer = new ByteCodec();
        }
        else if (valueClass.equals(Float.class) || valueClass.equals(float.class)) {
            serializer = new FloatCodec();
        }
        else if (valueClass.equals(Double.class) || valueClass.equals(double.class)) {
            serializer = new DoubleCodec();
        }
        else if (valueClass.equals(BigDecimal.class)) {
            serializer = new BigDecimalCodec();
        }
        else if (valueClass.equals(Boolean.class) || valueClass.equals(boolean.class)) {
            serializer = new BooleanCodec();
        }
        else if (valueClass.equals(byte[].class)) {
            serializer = new ByteArrayCodec();
        }
        else if (valueClass.equals(Date.class)) {
            serializer = new DateCodec();
        }
        else if (valueClass.equals(UUID.class)) {
            serializer = new UUIDCodec();
        }
        else {
            throw new IllegalArgumentException("Unsupported type " + valueClass);
        }
        return (Codec<T>)serializer;
    }
}
