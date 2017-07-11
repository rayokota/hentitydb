package io.hentitydb.store.hbase;

import io.hentitydb.serialization.Codec;
import io.hentitydb.store.TableMetadata;

public class HBaseUtil {

    public static <K, C> byte[] keyToBytes(K object, TableMetadata<K, C> metadata) {
        return keyToBytes(object, metadata.getKeyCodec());
    }

    public static <K, C> byte[] keyToBytes(K object, Codec<K> keyCodec) {
        if (object == null) return null;
        return keyCodec.encode(object);
    }
}
