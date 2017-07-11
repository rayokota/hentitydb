package io.hentitydb.serialization.tests;

import com.google.common.collect.ImmutableList;
import io.hentitydb.serialization.AbstractCodec;
import io.hentitydb.serialization.MigratingCodec;
import io.hentitydb.serialization.ReadBuffer;
import io.hentitydb.serialization.Version;
import io.hentitydb.serialization.WriteBuffer;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class MigratingCodecTest {
    @Version(1)
    static class VersionOneCodec extends AbstractCodec<String> {
        @Override
        public void encode(String value, WriteBuffer buffer) {
            buffer.writeVarInt(1);
            buffer.writeInt(2);
            buffer.writeUtf8String(value);
        }

        @Override
        public String decode(ReadBuffer buffer) {
            buffer.readVarInt();
            buffer.readInt();
            return buffer.readUtf8String();
        }
    }

    @Version(2)
    static class VersionTwoCodec extends AbstractCodec<String> {
        @Override
        public void encode(String value, WriteBuffer buffer) {
            buffer.writeUtf8String(value);
        }

        @Override
        public String decode(ReadBuffer buffer) {
            return buffer.readUtf8String();
        }
    }

    private final MigratingCodec<String> oldCodec = new MigratingCodec<>(ImmutableList.of(
            new VersionOneCodec()
    ));

    private final MigratingCodec<String> codec = new MigratingCodec<>(ImmutableList.of(
            new VersionOneCodec(), new VersionTwoCodec()
    ));

    @Test
    public void encodesValuesUsingTheMostRecentCodec() throws Exception {
        assertThat(codec.decode(codec.encode("yay")),
                   is("yay"));
    }

    @Test
    public void decodesValuesEncodedWithOlderCodecs() throws Exception {
        assertThat(codec.decode(oldCodec.encode("yay")),
                   is("yay"));
    }
}
