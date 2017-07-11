package io.hentitydb.entity;

import com.google.common.collect.Lists;
import io.hentitydb.serialization.LongCodec;
import io.hentitydb.serialization.StringCodec;
import io.hentitydb.store.CompareOp;
import io.hentitydb.store.KeyColumn;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class EntityFilterTest {
    private static Logger LOG = LoggerFactory.getLogger(EntityFilterTest.class);

    protected static LongCodec LONG_CODEC = new LongCodec();
    protected static StringCodec STRING_CODEC = new StringCodec();

    protected ColumnPredicate rawPredicate;
    protected EntityMapper<TestEntity, String> entityMapper;
    protected EntityFilter<String> filter;

    @Before
    public void setUp() throws Exception {
        ByteBuffer bb = ByteBuffer.wrap(STRING_CODEC.encode("foo"));
        rawPredicate = new ColumnPredicate()
                .setName("value2")
                .setOp(CompareOp.EQUAL)
                .setValue(bb);

        entityMapper = new EntityMapper<>(TestEntity.class);
        filter = new EntityFilter<>(
                entityMapper.getNumComponents(), rawPredicate, -1);
    }

    @Entity
    @Table(name = "testentity")
    public static class TestEntity {
        public TestEntity() {
        }

        public TestEntity(String rowKey, String part1, Long part2, Long value, String value2) {
            super();
            this.part1 = part1;
            this.part2 = part2;
            this.value = value;
            this.value2 = value2;
            this.rowKey = rowKey;
        }

        @Id
        String rowKey;      // This will be the row key
        @ElementId
        @Column
        String part1;       // This will be the first part of the composite
        @ElementId
        @Column
        Long   part2;       // This will be the second part of the composite
        @Column
        Long   value;       // This will be the value of the composite
        @Column
        String value2;       // This will be the value of the composite

        @Override
        public String toString() {
            return "TestEntity ["
                    +   "key="   + rowKey
                    + ", part1=" + part1
                    + ", part2=" + part2
                    + ", value=" + value
                    + ", value2=" + value2 + "]";
        }
    }

    @Test
    public void test() throws Exception {
        TestEntity entity1 = new TestEntity("A", "a", 1L, 2L, "foo");
        ByteBuffer columnName1 = entityMapper.toColumnName(entity1, "value");
        byte[] colName1 = new byte[columnName1.remaining()];
        columnName1.get(colName1);
        ByteBuffer columnName2 = entityMapper.toColumnName(entity1, "value2");
        byte[] colName2 = new byte[columnName2.remaining()];
        columnName2.get(colName2);

        TestEntity entity2 = new TestEntity("A", "b", 2L, 3L, "bar");
        ByteBuffer columnName3 = entityMapper.toColumnName(entity2, "value");
        byte[] colName3 = new byte[columnName3.remaining()];
        columnName3.get(colName3);
        ByteBuffer columnName4 = entityMapper.toColumnName(entity2, "value2");
        byte[] colName4 = new byte[columnName4.remaining()];
        columnName4.get(colName4);

        List<KeyColumn<String, byte[]>> keyColumns = Lists.newArrayList();

        TestColumn column = new TestColumn(colName1, LONG_CODEC.encode(entity1.value));
        KeyColumn<String, byte[]> keyColumn = new KeyColumn<>(STRING_CODEC, STRING_CODEC.encode(entity1.rowKey), column);
        keyColumns.add(keyColumn);

        TestColumn column2 = new TestColumn(colName2, STRING_CODEC.encode(entity1.value2));
        KeyColumn<String, byte[]> keyColumn2 = new KeyColumn<>(STRING_CODEC, STRING_CODEC.encode(entity1.rowKey), column2);
        keyColumns.add(keyColumn2);

        TestColumn column3 = new TestColumn(colName3, LONG_CODEC.encode(entity2.value));
        KeyColumn<String, byte[]> keyColumn3 = new KeyColumn<>(STRING_CODEC, STRING_CODEC.encode(entity2.rowKey), column3);
        keyColumns.add(keyColumn3);

        TestColumn column4 = new TestColumn(colName4, STRING_CODEC.encode(entity2.value2));
        KeyColumn<String, byte[]> keyColumn4 = new KeyColumn<>(STRING_CODEC, STRING_CODEC.encode(entity2.rowKey), column4);
        keyColumns.add(keyColumn4);

        Set<Integer> toKeep = filter.filterRow(keyColumns);
        int i = 0;
        for (Iterator<KeyColumn<String, byte[]>> iter = keyColumns.iterator(); iter.hasNext(); ) {
            iter.next();
            if (!toKeep.contains(i++)) iter.remove();
        }
        Assert.assertEquals(2, keyColumns.size());
    }
}
