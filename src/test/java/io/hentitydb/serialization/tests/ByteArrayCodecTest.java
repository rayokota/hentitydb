package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.ByteArrayCodec;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ByteArrayCodecTest {
    private final ByteArrayCodec codec = new ByteArrayCodec(true);
    private final ByteArrayCodec codec2 = new ByteArrayCodec(false);
    private final byte[] bytes = new byte[] { /*length = */ 42, 18, 1, 1, 1, 1, 1, -46, 1, -56, 1, -14, -4, -1, -1, -1, -1, -1, -1, -1, 1, 1 };
    private final byte[] bytes2 = new byte[] { 18, 1, 1, 1, 1, 1, -46, 1, -56, 1, -14, -4, -1, -1, -1, -1, -1, -1, -1, 1, 1 };

    @Test
    public void readsByteArrays() throws Exception {
        assertThat(codec.decode(bytes),
                   is(bytes2));
        assertThat(codec2.decode(bytes2),
                   is(bytes2));
    }

    @Test
    public void writesByteArrays() throws Exception {
        assertThat(codec.encode(bytes2),
                   is(bytes));
        assertThat(codec2.encode(bytes2),
                   is(bytes2));
    }
}
