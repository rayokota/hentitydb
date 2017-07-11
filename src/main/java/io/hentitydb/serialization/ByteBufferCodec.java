package io.hentitydb.serialization;

import java.nio.ByteBuffer;

public class ByteBufferCodec extends VarLengthCodec<ByteBuffer> {

    public ByteBufferCodec() {
        super();
    }

    public ByteBufferCodec(boolean encodeLength) {
        super(encodeLength);
    }

    @Override
    public int expectedSize(ByteBuffer value) {
        return value.remaining();
    }

    @Override
    public void encode(ByteBuffer value, WriteBuffer buffer) {
        byte[] bytes = new byte[value.remaining()];
        value.get(bytes);
        if (getEncodeLength()) buffer.writeVarInt(bytes.length);
        buffer.writeBytes(bytes);
    }

    @Override
    public ByteBuffer decode(ReadBuffer buffer) {
        if (getEncodeLength()) {
            final int size = buffer.readVarInt();
            return ByteBuffer.wrap(buffer.readBytes(size));
        } else {
            return ByteBuffer.wrap(buffer.readBytes());
        }
    }
}
