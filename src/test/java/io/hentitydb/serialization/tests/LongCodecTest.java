package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.LongCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class LongCodecTest {
    private final LongCodec codec = new LongCodec();
    private final byte[] bytes = new byte[]{ 0, 0, 0, 0, 0, 0, -100, 66 };

    @Test
    public void writesLongs() throws Exception {
        assertThat(codec.encode(40002L),
                   is(bytes));

        assertThat(Bytes.toBytes(40002L),
                   is(bytes));
    }

    @Test
    public void readsLongs() throws Exception {
        assertThat(codec.decode(bytes),
                   is(40002L));

        assertThat(Bytes.toLong(bytes),
                   is(40002L));
    }
}
