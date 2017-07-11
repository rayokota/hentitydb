package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.AbstractVersionedCodec;
import io.hentitydb.serialization.ReadBuffer;
import io.hentitydb.serialization.WriteBuffer;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class VersionedCodecTest {

    static class VersionedObj {
        public String s;
        public int i;
        public VersionedObj(String s, int i) {
            this.s = s;
            this.i = i;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VersionedObj that = (VersionedObj) o;

            if (i != that.i) return false;
            if (s != null ? !s.equals(that.s) : that.s != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = s != null ? s.hashCode() : 0;
            result = 31 * result + i;
            return result;
        }
    }

    static class VersionedCodec extends AbstractVersionedCodec<VersionedObj> {

        public int getLatestVersion() {
            return 1;
        }

        protected void encode(int version, VersionedObj value, WriteBuffer buffer) {
            if (version == 0) {
                buffer.writeUtf8String(value.s);
                buffer.writeVarInt(value.i);
            } else {
                buffer.writeVarInt(value.i);
                buffer.writeUtf8String(value.s);
            }
        }

        protected VersionedObj decode(int version, ReadBuffer buffer) {
            if (version == 0) {
                String s = buffer.readUtf8String();
                int i = buffer.readVarInt();
                return new VersionedObj(s, i);
            } else {
                int i = buffer.readVarInt();
                String s = buffer.readUtf8String();
                return new VersionedObj(s, i);
            }
        }

    }
    private final VersionedCodec codec = new VersionedCodec();
    private final byte[] bytes = new byte[]{ 2, 6, 4, 104, 105 };

    @Test
    public void writesVersionedObj() throws Exception {
        assertThat(codec.encode(new VersionedObj("hi", 3)),
                   is(bytes));
    }

    @Test
    public void writesVersionedObjWithBuffer() throws Exception {
        WriteBuffer buffer = new WriteBuffer(bytes.length);
        codec.encode(new VersionedObj("hi", 3), buffer);
        assertThat(buffer.finish(),
                is(bytes));
    }

    @Test
    public void readsReadVersionedObj() throws Exception {
        assertThat(codec.decode(bytes),
                   is(new VersionedObj("hi", 3)));
    }

    @Test
    public void readsVersionedObjWithBuffer() throws Exception {
        assertThat(codec.decode(new ReadBuffer(bytes)),
                is(new VersionedObj("hi", 3)));
    }

    @Test
    public void encodeDifferentVersionedObjs() throws Exception {
        VersionedObj obj = new VersionedObj("hi", 3);
        byte[] oldBytes = codec.encode(0, obj);
        assertThat(codec.decode(oldBytes),
                is(obj));
        byte[] newBytes = codec.encode(1, obj);
        assertThat(codec.decode(newBytes),
                is(obj));
    }
}
