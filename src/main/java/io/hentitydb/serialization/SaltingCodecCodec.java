package io.hentitydb.serialization;

/**
 * A {@link Codec} implementation which stores {@link SaltingCodec}s.
 */
public class SaltingCodecCodec<T> extends AbstractCodec<Codec<T>> {
    @Override
    public void encode(Codec<T> value, WriteBuffer buffer) {
        SaltingCodec<T> codec = (SaltingCodec<T>)value;
        CodecCodec<T> codecCodec = new CodecCodec<>();
        codecCodec.encode(codec.getCodec(), buffer);
        buffer.writeInt(codec.getNumBuckets());
    }

    @Override
    @SuppressWarnings("unchecked")
    public SaltingCodec<T> decode(ReadBuffer buffer) {
        CodecCodec<T> codecCodec = new CodecCodec<>();
        Codec<T> codec = codecCodec.decode(buffer);
        int numBuckets = buffer.readInt();
        return new SaltingCodec<>(codec, numBuckets);
    }
}
