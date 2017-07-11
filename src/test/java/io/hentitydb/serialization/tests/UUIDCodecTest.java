package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.UUIDCodec;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class UUIDCodecTest {
    private final UUIDCodec codec = new UUIDCodec();
    private final UUID uuid = new UUID(123L, 456L);
    private final byte[] bytes = new byte[]{ 0, 0, 0, 0, 0, 0, 0, 123, 0, 0, 0, 0, 0, 0, 1, -56 };

    @Test
    public void writesUUIDs() throws Exception {
        assertThat(codec.encode(uuid),
                   is(bytes));
    }

    @Test
    public void readsUUIDs() throws Exception {
        assertThat(codec.decode(bytes),
                   is(uuid));
    }
}
