package io.hentitydb.serialization;

public class ByteArrayCodec extends VarLengthCodec<byte[]> {

    public ByteArrayCodec() {
        super();
    }

    public ByteArrayCodec(boolean encodeLength) {
        super(encodeLength);
    }

    @Override
    public int expectedSize(byte[] value) {
        return value.length;
    }

    @Override
    public void encode(byte[] value, WriteBuffer buffer) {
        if (getEncodeLength()) buffer.writeVarInt(value.length);
        buffer.writeBytes(value);
    }

    @Override
    public byte[] decode(ReadBuffer buffer) {
        if (getEncodeLength()) {
            final int size = buffer.readVarInt();
            return buffer.readBytes(size);
        } else {
            return buffer.readBytes();
        }
    }
}
