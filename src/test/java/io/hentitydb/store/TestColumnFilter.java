package io.hentitydb.store;

import io.hentitydb.serialization.ReadBuffer;
import io.hentitydb.serialization.StringCodec;
import io.hentitydb.serialization.WriteBuffer;

public class TestColumnFilter extends AbstractFilter<String, String> {

    private String column;

    // Required for serialization
    public TestColumnFilter() {
    }

    public TestColumnFilter(String column) {
        this.column = column;
    }

    @Override
    public boolean filterKeyColumn(KeyColumn<String, String> keyColumn) {
        return !this.column.equals(keyColumn.getColumn().getName());
    }

    @Override
    public void encode(Filter<String, String> value, WriteBuffer buffer) {
        new StringCodec().encode(column, buffer);
    }

    @Override
    public Filter<String, String> decode(ReadBuffer buffer) {
        String column = new StringCodec().decode(buffer);
        return new TestColumnFilter(column);
    }
}
