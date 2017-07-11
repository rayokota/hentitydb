package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.InvertedLongCodec;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class InvertedLongCodecTest {
    private final InvertedLongCodec codec = new InvertedLongCodec();
    private final byte[] bytes = new byte[]{ 127, -1, -1, -1, -1, -1, 99, -67 };

    @Test
    public void writesLongs() throws Exception {
        assertThat(codec.encode(40002L),
                   is(bytes));
    }

    @Test
    public void readsLongs() throws Exception {
        assertThat(codec.decode(bytes),
                   is(40002L));
    }
}
