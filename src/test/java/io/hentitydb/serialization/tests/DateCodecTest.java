package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.DateCodec;
import org.junit.Test;

import java.util.Date;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DateCodecTest {
    private final DateCodec codec = new DateCodec();
    private final byte[] bytes = new byte[]{ 0, 0, 1, 76, -110, 96, -91, 74 };

    @Test
    public void writesDoubles() throws Exception {
        assertThat(codec.encode(new Date(1428384949578L)),
                   is(bytes));
    }

    @Test
    public void readsDoubles() throws Exception {
        assertThat(codec.decode(bytes),
                   is(new Date(1428384949578L)));
    }
}
