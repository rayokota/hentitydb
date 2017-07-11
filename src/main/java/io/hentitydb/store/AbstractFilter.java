package io.hentitydb.store;

import io.hentitydb.serialization.AbstractCodec;
import io.hentitydb.serialization.ReadBuffer;
import io.hentitydb.serialization.WriteBuffer;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class AbstractFilter<K, C> extends AbstractCodec<Filter<K, C>> implements Filter<K, C> {

    @Override
    public void reset() {
    }

    @Override
    public boolean ignoreRemainingRow() {
        return false;
    }

    @Override
    public byte[] transformKeyColumn(KeyColumn<K, C> keyColumn) {
        return null;
    }

    @Override
    public boolean hasFilterRow() {
        return false;
    }

    @Override
    public Set<Integer> filterRow(List<KeyColumn<K, C>> columns) {
        return Collections.emptySet();
    }

    @Override
    public void encode(Filter<K, C> value, WriteBuffer buffer) {
    }

    @Override
    public Filter<K, C> decode(ReadBuffer buffer) {
        return this;
    }
}
