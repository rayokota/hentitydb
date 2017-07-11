package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.SaltingCodec;
import io.hentitydb.serialization.StringCodec;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SaltingCodecTest {
    private final SaltingCodec<String> codec =
            new SaltingCodec<>(new StringCodec(true));
    private final byte[] bytes = new byte[]{ 82, 6, 119, 111, 111 };

    @Test
    public void writesStrings() throws Exception {
        assertThat(codec.encode("woo"),
                   is(bytes));
    }

    @Test
    public void readsStrings() throws Exception {
        assertThat(codec.decode(bytes),
                   is("woo"));
    }
}
