package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.VarIntCodec;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class VarIntCodecTest {
    private final VarIntCodec codec = new VarIntCodec();
    private final byte[] bytes = new byte[]{ -124, -15, 4 };

    @Test
    public void writesInts() throws Exception {
        assertThat(codec.encode(40002),
                   is(bytes));
    }

    @Test
    public void readsInts() throws Exception {
        assertThat(codec.decode(bytes),
                   is(40002));
    }
}
