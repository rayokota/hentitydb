package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.DoubleCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DoubleCodecTest {
    private final DoubleCodec codec = new DoubleCodec();
    private final byte[] bytes = new byte[]{ 64, 40, -80, -5, -88, -126, 106, -87 };

    @Test
    public void writesDoubles() throws Exception {
        assertThat(codec.encode(12.34567d),
                   is(bytes));

        assertThat(Bytes.toBytes(12.34567d),
                   is(bytes));
    }

    @Test
    public void readsDoubles() throws Exception {
        assertThat(codec.decode(bytes),
                   is(12.34567d));

        assertThat(Bytes.toDouble(bytes),
                   is(12.34567d));
    }
}
