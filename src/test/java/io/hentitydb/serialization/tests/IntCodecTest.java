package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.IntCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class IntCodecTest {
    private final IntCodec codec = new IntCodec();
    private final byte[] bytes = new byte[]{ 0, 0, -100, 66 };

    @Test
    public void writesInts() throws Exception {
        assertThat(codec.encode(40002),
                   is(bytes));

        assertThat(Bytes.toBytes(40002),
                   is(bytes));
    }

    @Test
    public void readsInts() throws Exception {
        assertThat(codec.decode(bytes),
                   is(40002));

        assertThat(Bytes.toInt(bytes),
                   is(40002));
    }
}
