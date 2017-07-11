package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.ClassCodec;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ClassCodecTest {
    private final ClassCodec codec = new ClassCodec();
    private final byte[] bytes = new byte[]{32, 106, 97, 118, 97, 46, 108, 97, 110, 103, 46, 83, 116, 114, 105, 110, 103};

    @Test
    public void writesClass() throws Exception {
        assertThat(codec.encode(String.class),
                   is(bytes));
    }

    @Test
    public void readsClass() throws Exception {
        assertThat(codec.decode(bytes).getName(),
                   is(String.class.getName()));
    }
}
