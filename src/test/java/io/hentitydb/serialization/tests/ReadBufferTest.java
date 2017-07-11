package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.ReadBuffer;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ReadBufferTest {
    @Test
    public void deserializingASingleByte() throws Exception {
        final ReadBuffer buffer = new ReadBuffer(new byte[] { 1 });

        assertThat("returns a byte",
                   buffer.readByte(),
                   is((byte) 1));
    }

    @Test
    public void deserializingAShort() throws Exception {
        final ReadBuffer buffer = new ReadBuffer(new byte[]{ 3, 32 });

        assertThat("returns a short",
                   buffer.readShort(),
                   is((short) 800));
    }

    @Test
    public void deserializingAnInt() throws Exception {
        final ReadBuffer buffer = new ReadBuffer(new byte[]{0, 0, -81, -56});

        assertThat("returns an int",
                   buffer.readInt(),
                   is(45000));
    }

    @Test
    public void deserializingALong() throws Exception {
        final ReadBuffer buffer = new ReadBuffer(new byte[]{0, 0, 0, 1, 42, 5, -14, 0});

        assertThat("returns a long",
                   buffer.readLong(),
                   is(5000000000L));
    }

    @Test
    public void deserializingAVarInt() throws Exception {
        final ReadBuffer buffer = new ReadBuffer(new byte[]{-112, -65, 5});

        assertThat("returns an int",
                   buffer.readVarInt(),
                   is(45000));
    }

    @Test
    public void deserializingAVarLong() throws Exception {
        final ReadBuffer buffer = new ReadBuffer(new byte[]{-128, -56, -81, -96, 37});

        assertThat("returns a long",
                   buffer.readVarLong(),
                   is(5000000000L));
    }

    @Test
    public void deserializingAFixedLengthSeriesOfBytes() throws Exception {
        final ReadBuffer buffer = new ReadBuffer(new byte[]{1, 2, 3, 4});

        assertThat("returns a byte array",
                   buffer.readBytes(3),
                   is(new byte[]{1, 2, 3}));
    }

    @Test
    public void deserializingASeriesOfBytes() throws Exception {
        final ReadBuffer buffer = new ReadBuffer(new byte[]{1, 2, 3, 4});

        assertThat("returns a byte array",
                   buffer.readBytes(),
                   is(new byte[]{1, 2, 3, 4}));
    }

    @Test
    public void deserializingAUtf8EncodedString() throws Exception {
        final ReadBuffer buffer = new ReadBuffer(new byte[]{18, 104, 101, 108, 108, 111, 32, 109, 111, 109});

        assertThat("returns a string",
                   buffer.readUtf8String(),
                   is("hello mom"));
    }

    @Test
    public void deserializingAFloat() throws Exception {
        final ReadBuffer buffer = new ReadBuffer(new byte[]{ 62, -47, -21, -123 });

        assertThat("returns a float",
                   buffer.readFloat(),
                   is(0.41f));
    }

    @Test
    public void deserializingADouble() throws Exception {
        final ReadBuffer buffer = new ReadBuffer(new byte[]{ 63, -38, 61, 112, -93, -41, 10, 61 });

        assertThat("returns a double",
                   buffer.readDouble(),
                   is(0.41)
        );
    }
}
