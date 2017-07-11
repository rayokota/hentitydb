package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.VarIntArrayCodec;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class VarIntArrayCodecTest {
    private final VarIntArrayCodec codec = new VarIntArrayCodec();
    private final int[] values = { 1, 2, 3, 4, 5, -100, -200, Integer.MAX_VALUE, Integer.MIN_VALUE };
    private final byte[] bytes = { 18, 1, 1, 1, 1, 1, -46, 1, -56, 1, -14, -4, -1, -1, 15, 1 };

    @Test
    public void readsInts() throws Exception {
        assertThat(codec.decode(bytes),
                   is(values));
    }

    @Test
    public void writesInts() throws Exception {
        assertThat(codec.encode(values),
                   is(bytes));
    }
}
