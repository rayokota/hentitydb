package io.hentitydb.serialization;

import javax.persistence.EnumType;

/**
 * A {@link Codec} implementation which stores {@link Enum}s.
 */
public class EnumCodec<E extends Enum<E>> extends AbstractCodec<Enum<E>> {
    private final Class<E> enumClass;
    private final EnumType enumType;

    public EnumCodec(Class<E> enumClass, EnumType enumType) {
        this.enumClass = enumClass;
        this.enumType = enumType;
    }

    public Class<E> getEnumClass() {
        return enumClass;
    }

    public EnumType getEnumType() {
        return enumType;
    }

    @Override
    public int expectedSize(Enum<E> value) {
        return 4;
    }

    @Override
    public void encode(Enum<E> value, WriteBuffer buffer) {
        switch (enumType) {
            case ORDINAL:
                buffer.writeInt(value.ordinal());
                break;
            case STRING:
                buffer.writeUtf8String(value.name());
                break;
            default:
                throw new IllegalArgumentException("Unsupported enumType " + enumType);
        }
    }

    @Override
    public Enum<E> decode(ReadBuffer buffer) {
        switch (enumType) {
            case ORDINAL:
                return fromOrdinal(buffer.readInt());
            case STRING:
                return fromName(buffer.readUtf8String());
            default:
                throw new IllegalArgumentException("Unsupported enumType " + enumType);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Codec<Codec<Enum<E>>> getSelfCodec() {
        return new EnumCodecCodec();
    }

    private E fromOrdinal(int ordinal) {
        final E[] enumsByOrdinal = enumClass.getEnumConstants();
        if (ordinal < 0 || ordinal >= enumsByOrdinal.length) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unknown ordinal value [%s] for enum class [%s]",
                            ordinal,
                            enumClass.getName()
                    )
            );
        }
        return enumsByOrdinal[ordinal];
    }

    private E fromName(String name) {
        if (name == null) return null;
        return Enum.valueOf(enumClass, name.trim());
    }
}
