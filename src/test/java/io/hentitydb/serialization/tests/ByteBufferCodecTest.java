package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.ByteBufferCodec;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ByteBufferCodecTest {
    private final ByteBufferCodec codec = new ByteBufferCodec(true);
    private final ByteBufferCodec codec2 = new ByteBufferCodec(false);
    private final byte[] bytes = new byte[] { /*length = */ 42, 18, 1, 1, 1, 1, 1, -46, 1, -56, 1, -14, -4, -1, -1, -1, -1, -1, -1, -1, 1, 1 };
    private final byte[] bytes2 = new byte[] { 18, 1, 1, 1, 1, 1, -46, 1, -56, 1, -14, -4, -1, -1, -1, -1, -1, -1, -1, 1, 1 };

    @Test
    public void readsByteArrays() throws Exception {
        assertThat(codec.decode(bytes).array(),
                   is(bytes2));
        assertThat(codec2.decode(bytes2).array(),
                   is(bytes2));
    }

    @Test
    public void writesByteArrays() throws Exception {
        assertThat(codec.encode(ByteBuffer.wrap(bytes2)),
                   is(bytes));
        assertThat(codec2.encode(ByteBuffer.wrap(bytes2)),
                   is(bytes2));
    }
}
