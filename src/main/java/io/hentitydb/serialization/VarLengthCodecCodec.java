package io.hentitydb.serialization;

import com.google.common.base.Throwables;

/**
 * A {@link Codec} implementation which stores {@link VarLengthCodec}s.
 */
public class VarLengthCodecCodec<T> extends AbstractCodec<Codec<T>> {
    @Override
    public void encode(Codec<T> value, WriteBuffer buffer) {
        VarLengthCodec<T> codec = (VarLengthCodec<T>)value;
        ClassCodec classCodec = new ClassCodec();
        classCodec.encode(codec.getClass(), buffer);
        buffer.writeByte(codec.getEncodeLength() ? 1 : 0);
    }

    @Override
    @SuppressWarnings("unchecked")
    public VarLengthCodec<T> decode(ReadBuffer buffer) {
        try {
            ClassCodec classCodec = new ClassCodec();
            final Class<VarLengthCodec<T>> cls = classCodec.decode(buffer);
            final boolean encodeLength = buffer.readByte() == 1;
            return cls.getConstructor(boolean.class).newInstance(encodeLength);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
