package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.StringCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class StringCodecTest {
    private final StringCodec codec = new StringCodec(true);
    private final StringCodec codec2 = new StringCodec(false);
    private final byte[] bytes = new byte[]{6, 119, 111, 111};
    private final byte[] bytes2 = new byte[]{119, 111, 111};

    @Test
    public void writesStrings() throws Exception {
        assertThat(codec.encode("woo"),
                   is(bytes));
        assertThat(codec2.encode("woo"),
                   is(bytes2));

        assertThat(Bytes.toBytes("woo"),
                   is(bytes2));
    }

    @Test
    public void readsStrings() throws Exception {
        assertThat(codec.decode(bytes),
                   is("woo"));
        assertThat(codec2.decode(bytes2),
                   is("woo"));

        assertThat(Bytes.toString(bytes2),
                   is("woo"));
    }
}
