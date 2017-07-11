package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.ShortCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ShortCodecTest {
    private final ShortCodec codec = new ShortCodec();
    private final byte[] bytes = new byte[]{ 1, -110 };

    @Test
    public void writesShorts() throws Exception {
        assertThat(codec.encode((short)402),
                   is(bytes));

        assertThat(Bytes.toBytes((short)402),
                   is(bytes));
    }

    @Test
    public void readsShorts() throws Exception {
        assertThat(codec.decode(bytes),
                   is((short)402));

        assertThat(Bytes.toShort(bytes),
                   is((short)402));
    }
}
