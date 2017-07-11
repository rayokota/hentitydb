package io.hentitydb.serialization;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.SortedMap;

public class MigratingCodec<T> extends AbstractCodec<T> {
    private final int[] versions;
    private final Codec<T>[] codecs;

    @SuppressWarnings("unchecked")
    public MigratingCodec(Iterable<? extends Codec<T>> codecs) {
        final SortedMap<Integer, Codec<T>> sorted = Maps.newTreeMap();
        for (Codec<T> codec : codecs) {
            final Version version = codec.getClass().getAnnotation(Version.class);
            if (version == null) {
                throw new IllegalArgumentException(codec.getClass().getCanonicalName() + " is not annotated with @Version");
            }
            sorted.put(version.value(), codec);
        }

        this.versions = new int[sorted.size()];
        this.codecs = new Codec[sorted.size()];
        int i = 0;
        for (Map.Entry<Integer, Codec<T>> entry : sorted.entrySet()) {
            final int index = i++;
            this.codecs[index] = entry.getValue();
            this.versions[index] = entry.getKey();
        }
    }

    @Override
    public void encode(T value, WriteBuffer buffer) {
        final Codec<T> current = codecs[0];
        buffer.writeVarInt(versions[0]);
        current.encode(value, buffer);
    }

    @Override
    public T decode(ReadBuffer buffer) {
        final int version = buffer.readVarInt();

        for (int i = 0; i < versions.length; i++) {
            if (versions[i] == version) {
                return codecs[i].decode(buffer);
            }
        }
        throw new RuntimeException("WHOOPS");
    }
}
