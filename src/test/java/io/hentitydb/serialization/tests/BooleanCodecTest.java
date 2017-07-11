package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.BooleanCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BooleanCodecTest {
    private final BooleanCodec codec = new BooleanCodec();
    private final byte[] bytes = new byte[]{ -1 };

    @Test
    public void writesBooleans() throws Exception {
        assertThat(codec.encode(true),
                   is(bytes));

        assertThat(Bytes.toBytes(true),
                   is(bytes));
    }

    @Test
    public void readsBooleans() throws Exception {
        assertThat(codec.decode(bytes),
                   is(true));

        assertThat(Bytes.toBoolean(bytes),
                   is(true));
    }
}
