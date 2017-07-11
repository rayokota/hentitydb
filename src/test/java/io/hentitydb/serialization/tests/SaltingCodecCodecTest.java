package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.SaltingCodec;
import io.hentitydb.serialization.SaltingCodecCodec;
import io.hentitydb.serialization.StringCodec;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SaltingCodecCodecTest {
    private final SaltingCodecCodec<String> codec = new SaltingCodecCodec<>();
    private final byte[] bytes = new byte[]{76, 105, 111, 46, 104, 101, 110, 116, 105, 116, 121, 100, 98, 46, 115, 101,
            114, 105, 97, 108, 105, 122, 97, 116, 105, 111, 110, 46, 83, 116, 114, 105, 110, 103, 67, 111, 100, 101, 99,
            0, 0, 1, 0};

    @Test
    public void writesCodec() throws Exception {
        assertThat(codec.encode(new SaltingCodec<>(new StringCodec(true))),
                is(bytes));
    }

    @Test
    public void readsCodec() throws Exception {
        assertThat(codec.decode(bytes).getClass().getName(),
                is(SaltingCodec.class.getName()));
    }
}
