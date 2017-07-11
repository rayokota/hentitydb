package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.InvertedIntCodec;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class InvertedIntCodecTest {
    private final InvertedIntCodec codec = new InvertedIntCodec();
    private final byte[] bytes = new byte[]{ 127, -1, 99, -67 };

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
