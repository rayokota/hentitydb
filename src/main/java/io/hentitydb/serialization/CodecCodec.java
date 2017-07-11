package io.hentitydb.serialization;

import com.google.common.base.Throwables;

/**
 * A {@link Codec} implementation which stores {@link Codec}s.
 */
public class CodecCodec<T> extends AbstractCodec<Codec<T>> {
    @Override
    public void encode(Codec<T> value, WriteBuffer buffer) {
        ClassCodec classCodec = new ClassCodec();
        classCodec.encode(value.getClass(), buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Codec<T> decode(ReadBuffer buffer) {
        try {
            ClassCodec classCodec = new ClassCodec();
            final Class cls = classCodec.decode(buffer);
            return (Codec<T>)cls.newInstance();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
