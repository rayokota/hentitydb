package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.BufferRecycler;
import io.hentitydb.serialization.WriteBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.lang.System.arraycopy;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WriteBufferTest {
    final WriteBuffer buffer = new WriteBuffer(10);
    final int prefixSize = 16 * 1024;
    final byte[] prefix = new byte[prefixSize];

    @Before
    public void setUp() throws Exception {
        buffer.writeBytes(prefix);
    }

    @After
    public void tearDown() throws Exception {
        BufferRecycler.recycler().reset();
    }

    @Test
    public void serializesSingleBytes() throws Exception {
        buffer.writeByte(1);

        assertThat(buffer.finish(),
                   is(prefixed(new byte[]{1})));
    }

    @Test
    public void serializesShorts() throws Exception {
        buffer.writeShort(800);

        assertThat(buffer.finish(),
                   is(prefixed(new byte[]{3, 32})));
    }

    @Test
    public void serializesInts() throws Exception {
        buffer.writeInt(45000);

        assertThat(buffer.finish(),
                   is(prefixed(new byte[]{0, 0, -81, -56})));
    }

    @Test
    public void serializesLongs() throws Exception {
        buffer.writeLong(5000000000L);

        assertThat(buffer.finish(),
                   is(prefixed(new byte[]{0, 0, 0, 1, 42, 5, -14, 0})));
    }

    @Test
    public void serializesVarInts() throws Exception {
        buffer.writeVarInt(45000);

        assertThat(buffer.finish(),
                   is(prefixed(new byte[]{-112, -65, 5})));
    }

    @Test
    public void serializesVarLongs() throws Exception {
        buffer.writeVarLong(5000000000L);

        assertThat(buffer.finish(),
                   is(prefixed(new byte[]{-128, -56, -81, -96, 37})));
    }

    @Test
    public void serializesByteArrays() throws Exception {
        buffer.writeBytes(new byte[]{1, 2, 3});
        
        assertThat(buffer.finish(),
                   is(prefixed(new byte[]{1, 2, 3})));
    }

    @Test
    public void serializesSlicesOfByteArrays() throws Exception {
        buffer.writeBytes(new byte[]{1, 2, 3}, 1, 1);

        assertThat(buffer.finish(),
                   is(prefixed(new byte[]{ 2 })));
    }

    @Test
    public void serializesFloats() throws Exception {
        buffer.writeFloat(0.41f);

        assertThat(buffer.finish(),
                   is(prefixed(new byte[]{ 62, -47, -21, -123 })));
    }

    @Test
    public void serializesDoubles() throws Exception {
        buffer.writeDouble(0.41);

        assertThat(buffer.finish(),
                   is(prefixed(new byte[]{ 63, -38, 61, 112, -93, -41, 10, 61 })));
    }

    @Test
    public void writesUtf8Strings() throws Exception {
        buffer.writeUtf8String("hello mom");
        
        assertThat(buffer.finish(),
                   is(prefixed(new byte[]{18, 104, 101, 108, 108, 111, 32, 109, 111, 109})));
    }
    
    private byte[] prefixed(byte[] bytes) {
        final byte[] prefixed = new byte[bytes.length + prefixSize];
        arraycopy(bytes, 0, prefixed, prefixSize, bytes.length);
        return prefixed;
    }
}
