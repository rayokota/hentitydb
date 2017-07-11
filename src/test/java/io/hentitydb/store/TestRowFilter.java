package io.hentitydb.store;

import io.hentitydb.serialization.ReadBuffer;
import io.hentitydb.serialization.StringCodec;
import io.hentitydb.serialization.WriteBuffer;

public class TestRowFilter extends AbstractFilter<String, String> {

    private String key;

    // Required for serialization
    public TestRowFilter() {
    }

    public TestRowFilter(String key) {
        this.key = key;
    }

    @Override
    public boolean filterKeyColumn(KeyColumn<String, String> keyColumn) {
        return !this.key.equals(keyColumn.getKey());
    }

    @Override
    public void encode(Filter<String, String> value, WriteBuffer buffer) {
        new StringCodec().encode(key, buffer);
    }

    @Override
    public Filter<String, String> decode(ReadBuffer buffer) {
        String column = new StringCodec().decode(buffer);
        return new TestRowFilter(column);
    }
}
