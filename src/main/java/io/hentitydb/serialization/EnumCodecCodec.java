package io.hentitydb.serialization;

import javax.persistence.EnumType;

/**
 * A {@link Codec} implementation which stores {@link EnumCodec}s.
 */
public class EnumCodecCodec<T extends Enum<T>> extends AbstractCodec<EnumCodec<T>> {
    private final static ClassCodec CLASS_CODEC = new ClassCodec();

    @Override
    public void encode(EnumCodec<T> value, WriteBuffer buffer) {
        CLASS_CODEC.encode(value.getEnumClass());
        buffer.writeByte(value.getEnumType() == EnumType.ORDINAL ? 1 : 0);
    }

    @Override
    @SuppressWarnings("unchecked")
    public EnumCodec<T> decode(ReadBuffer buffer) {
        Class<T> enumClass = CLASS_CODEC.decode(buffer);
        EnumType enumType = buffer.readByte() == 1 ? EnumType.ORDINAL : EnumType.STRING;
        return new EnumCodec(enumClass, enumType);
    }
}
