package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.FloatCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class FloatCodecTest {
    private final FloatCodec codec = new FloatCodec();
    private final byte[] bytes = new byte[]{ 65, 69, -123, 31 };

    @Test
    public void writesFloats() throws Exception {
        assertThat(codec.encode(12.345f),
                   is(bytes));

        assertThat(Bytes.toBytes(12.345f),
                   is(bytes));
    }

    @Test
    public void readsFloats() throws Exception {
        assertThat(codec.decode(bytes),
                   is(12.345f));

        assertThat(Bytes.toFloat(bytes),
                   is(12.345f));
    }
}
