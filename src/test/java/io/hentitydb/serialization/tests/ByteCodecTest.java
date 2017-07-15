package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.ByteCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ByteCodecTest {
    private final ByteCodec codec = new ByteCodec();
    private final byte[] bytes = new byte[]{ 110 };

    @Test
    public void writesByte() throws Exception {
        assertThat(codec.encode((byte)110),
                   is(bytes));
    }

    @Test
    public void readsByte() throws Exception {
        assertThat(codec.decode(bytes),
                   is((byte)110));
    }
}
