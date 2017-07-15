package io.hentitydb.entity;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.hentitydb.Configuration;
import io.hentitydb.Environment;
import io.hentitydb.serialization.LongCodec;
import io.hentitydb.serialization.StringCodec;
import io.hentitydb.serialization.VarIntArrayCodec;
import io.hentitydb.store.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.*;
import javax.persistence.Column;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public abstract class EntityContextTest {
    private static Logger LOG = LoggerFactory.getLogger(EntityContextTest.class);

    protected static Configuration config;
    protected static ConnectionFactory factory;
    protected static Connection conn;
    protected static EntityContext<TestEntity, String> manager;
    protected static EntityContext<TestEntityRefing, String> managerRefing;
    protected static EntityContext<TestEntityIndexing, String> managerIndexing;
    protected static EntityContext<TestEntityWithOrdering, String> manager2;
    protected static EntityContext<TestEntityWithNoElementIds, String> manager3;
    protected static EntityContext<TestEntityWithNoColumns, String> manager4;
    protected static EntityContext<TestEntityWithColumnFamilies, String> manager5;
    protected static EntityContext<TestEntityWithTTL, String> manager6;
    protected static EntityContext<TestEntityWithTypes, String> managerTypes;
    protected static EntityContext<PInboxEntry, String> managerInbox;

    @Before
    public void setUp() throws Exception {
        manager = Environment.getEntityContext(conn, TestEntity.class);

        for (long i = 0; i < 10; i++) {
            manager.put(new TestEntity("A", "a", i, i*i, String.valueOf(i)));
            manager.put(new TestEntity("A", "b", i, i*i, String.valueOf(i)));
            manager.put(new TestEntity("B", "a", i, i*i, String.valueOf(i)));
            manager.put(new TestEntity("B", "b", i, i*i, String.valueOf(i)));
        }

        manager.put(new TestEntity("C", "c", 1L, null, null));
        manager.put(new TestEntity("C", "c", 2L, 2L, null));
        manager.put(new TestEntity("C", "c", 3L, null, "3"));

        manager.put(new TestEntity("D", "abcdefghijklmnopqrstuvwxyz", 1L, 1L, "1"));
        manager.put(new TestEntity("D", "xyz", 2L, 2L, "2"));
        manager.put(new TestEntity("D", "m", 3L, 3L, "3"));

        manager.put(new TestEntity("E", "e", 4L, 4L, "4"));
        manager.put(new TestEntity("E", "f", 5L, 5L, null));

        managerRefing = Environment.getEntityContext(conn, TestEntityRefing.class);

        for (long i = 0; i < 10; i++) {
            managerRefing.put(new TestEntityRefing("c", "A", "a", i, i*i, String.valueOf(i)));
            managerRefing.put(new TestEntityRefing("c", "B", "b", i, i*i, String.valueOf(i)));

            if (i > 0) {
                managerRefing.put(new TestEntityRefing("ref", "A", "a", i, i * i, String.valueOf(i)));
                managerRefing.put(new TestEntityRefing("ref", "B", "b", i, i * i, String.valueOf(i)));
            }
        }

        managerIndexing = Environment.getEntityContext(conn, TestEntityIndexing.class);

        for (long i = 0; i < 10; i++) {
            managerIndexing.put(new TestEntityIndexing("c", "A", i, i*i, String.valueOf(i)));
            managerIndexing.put(new TestEntityIndexing("c", "B", i, i*i, String.valueOf(i)));

            if (i < 9) {  // since descending
                managerIndexing.put(new TestEntityIndexing("ix", "A", i*i, i, String.valueOf(i)));
                managerIndexing.put(new TestEntityIndexing("ix", "B", i*i, i, String.valueOf(i)));
            }
        }

        manager2 = Environment.getEntityContext(conn, TestEntityWithOrdering.class);

        Collection<TestEntityWithOrdering> entities = Lists.newArrayList();
        for (long i = 0; i < 10; i++) {
            entities.add(new TestEntityWithOrdering("A", "a", i, i*i, String.valueOf(i)));
            entities.add(new TestEntityWithOrdering("A", "b", i, i*i, String.valueOf(i)));
            entities.add(new TestEntityWithOrdering("B", "a", i, i*i, String.valueOf(i)));
            entities.add(new TestEntityWithOrdering("B", "b", i, i*i, String.valueOf(i)));
        }
        manager2.put(entities);

        manager3 = Environment.getEntityContext(conn, TestEntityWithNoElementIds.class);

        manager3.put(new TestEntityWithNoElementIds("A", 10L, "100", new int[] { 1, 2 }));
        manager3.put(new TestEntityWithNoElementIds("B", 20L, "200", new int[] { 3, 4, 5}));

        manager4 = Environment.getEntityContext(conn, TestEntityWithNoColumns.class);

        manager4.put(new TestEntityWithNoColumns("A", 10L, "100"));
        manager4.put(new TestEntityWithNoColumns("B", 20L, "200"));

        manager5 = Environment.getEntityContext(conn, TestEntityWithColumnFamilies.class);

        manager5.put(new TestEntityWithColumnFamilies("c", "C", "c1", 10L, 100L, "100"));
        manager5.put(new TestEntityWithColumnFamilies("c", "C", "c2", 11L, 101L, "101"));
        manager5.put(new TestEntityWithColumnFamilies("d", "C", "c3", 12L, 102L, "102"));
        manager5.put(new TestEntityWithColumnFamilies("d", "D", "d", 20L, 200L, "200"));

        manager6 = Environment.getEntityContext(conn, TestEntityWithTTL.class);

        managerTypes = Environment.getEntityContext(conn, TestEntityWithTypes.class);

        managerInbox = Environment.getEntityContext(conn, PInboxEntry.class);
    }

    @After
    public void tearDown() throws Exception {
        if (manager != null) manager.truncate();
        if (manager2 != null) manager2.truncate();
        if (manager3 != null) manager3.truncate();
        if (manager4 != null) manager4.truncate();
        if (manager5 != null) manager5.truncate();
        if (manager6 != null) manager6.truncate();
        if (managerInbox != null) managerInbox.truncate();
    }

    @Entity
    @javax.persistence.Table(name = "testentity")
    @ColumnFamilies(
            @ColumnFamily(name = "c", maxEntitiesPerRow = 5)
    )
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

    @Entity
    @javax.persistence.Table(name = "testentityrefing")
    @ColumnFamilies({
            @ColumnFamily(name = "c", maxEntitiesPerRow = 5, referencingFamily = "ref"),
            @ColumnFamily(name = "ref")
    })
    public static class TestEntityRefing {
        public TestEntityRefing() {
        }

        public TestEntityRefing(String family, String rowKey, String part1, Long part2, Long value, String value2) {
            super();
            this.family = family;
            this.part1 = part1;
            this.part2 = part2;
            this.value = value;
            this.value2 = value2;
            this.rowKey = rowKey;
        }

        @ColumnFamilyName
        String family;

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
            return "TestEntityRefing ["
                    +   "family=" + family
                    + ", key="   + rowKey
                    + ", part1=" + part1
                    + ", part2=" + part2
                    + ", value=" + value
                    + ", value2=" + value2 + "]";
        }
    }

    @Entity
    @javax.persistence.Table(name = "testentityindexing")
    @ColumnFamilies({
            @ColumnFamily(name = "c", maxEntitiesPerRow = 5, indexingFamily = "ix", indexingValueName = "value"),
            @ColumnFamily(name = "ix")
    })
    public static class TestEntityIndexing {
        public TestEntityIndexing() {
        }

        public TestEntityIndexing(String family, String rowKey, Long part, Long value, String value2) {
            super();
            this.family = family;
            this.part = part;
            this.value = value;
            this.value2 = value2;
            this.rowKey = rowKey;
        }

        @ColumnFamilyName
        String family;

        @Id
        String rowKey;      // This will be the row key
        @ElementId
        @Column @OrderBy("DESC")
        Long   part;        // Only one part in the composite is supported and it must be a Long
        @Column
        Long   value;       // This will be the value of the composite
        @Column
        String value2;       // This will be the value of the composite

        @Override
        public String toString() {
            return "TestEntityIndexing ["
                    +   "family=" + family
                    + ", key="   + rowKey
                    + ", part=" + part
                    + ", value=" + value
                    + ", value2=" + value2 + "]";
        }
    }

    @Entity
    @javax.persistence.Table(name = "testentity2")
    public static class TestEntityWithOrdering {
        public TestEntityWithOrdering() {
        }

        public TestEntityWithOrdering(String rowKey, String part1, Long part2, Long value, String value2) {
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
        @Column @OrderBy("DESC")
        Long   part2;       // This will be the second part of the composite
        @Column
        Long   value;       // This will be the value of the composite
        @Column
        String value2;       // This will be the value of the composite

        @Override
        public String toString() {
            return "TestEntityWithOrdering ["
                    +   "key="   + rowKey
                    + ", part1=" + part1
                    + ", part2=" + part2
                    + ", value=" + value
                    + ", value2=" + value2 + "]";
        }
    }

    @Entity
    @javax.persistence.Table(name = "testentity3")
    public static class TestEntityWithNoElementIds {
        public TestEntityWithNoElementIds() {
        }

        public TestEntityWithNoElementIds(String rowKey, Long value, String value2, int[] value3) {
            super();
            this.value = value;
            this.value2 = value2;
            this.value3 = value3.clone();
            this.rowKey = rowKey;
        }

        @Id
        String rowKey;      // This will be the row key
        @Column
        Long   value;       // This will be the value of the composite
        @Column
        String value2;       // This will be the value of the composite
        @Column @Codec(VarIntArrayCodec.class)
        int[] value3;       // This will be the value of the composite

        @Override
        public String toString() {
            return "TestEntityWithNoElementIds ["
                    +   "key="   + rowKey
                    + ", value=" + value
                    + ", value2=" + value2
                    + ", value3=" + Arrays.toString(value3) + "]";
        }
    }

    @Entity
    @javax.persistence.Table(name = "testentity4")
    public static class TestEntityWithNoColumns {
        public TestEntityWithNoColumns() {
        }
        
        public TestEntityWithNoColumns(String rowKey, Long value, String value2) {
            super();
            this.value = value;
            this.value2 = value2;
            this.rowKey = rowKey;
        }
        
        @Id
        String rowKey;      // This will be the row key
        @ElementId
        @Column
        Long   value;       // This will be the value of the composite
        @ElementId
        @Column
        String value2;       // This will be the value of the composite

        @Override
        public String toString() {
            return "TestEntityWithNoColumns ["
                    +   "key="   + rowKey 
                    + ", value=" + value
                    + ", value2=" + value2 + "]";
        }
    }

    @Entity
    @javax.persistence.Table(name = "testentity5")
    @ColumnFamilies({
            @ColumnFamily(name = "c"), @ColumnFamily(name = "d")
    })
    public static class TestEntityWithColumnFamilies {
        public TestEntityWithColumnFamilies() {
        }

        public TestEntityWithColumnFamilies(String family, String rowKey, String part1, Long part2, Long value, String value2) {
            super();
            this.family = family;
            this.part1 = part1;
            this.part2 = part2;
            this.value = value;
            this.value2 = value2;
            this.rowKey = rowKey;
        }

        @ColumnFamilyName
        String family;
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
            return "TestEntityWithColumnFamilies ["
                    +   "key="   + rowKey
                    + ", part1=" + part1
                    + ", part2=" + part2
                    + ", value=" + value
                    + ", value2=" + value2 + "]";
        }
    }

    @Entity
    @javax.persistence.Table(name = "testentityttl")
    @ColumnFamilies(
            @ColumnFamily(name = "c")
    )
    public static class TestEntityWithTTL {
        public TestEntityWithTTL() {
        }

        public TestEntityWithTTL(String id, String column) {
            this.id = id;
            this.column = column;
        }

        @Id
        String id;
        @Column
        String column;

        @SuppressWarnings("unused")
        @TTL
        public Integer getTTL() {
            return 2000;
        }
    }

    enum TestEnum { ENUM1, ENUM2, ENUM3 }

    @Entity
    @javax.persistence.Table(name = "testentitytypes")
    public static class TestEntityWithTypes {
        public TestEntityWithTypes() {
        }

        public TestEntityWithTypes(String rowKey,
                                   String str,
                                   Byte b,
                                   Short s,
                                   Integer i,
                                   Long l,
                                   Float f,
                                   Double d,
                                   BigDecimal bd,
                                   Boolean bool,
                                   byte[] bytes,
                                   Date date,
                                   UUID uuid,
                                   TestEnum e1,
                                   TestEnum e2) {
            this.rowKey = rowKey;
            this.str = str;
            this.b = b;
            this.s = s;
            this.i = i;
            this.l = l;
            this.f = f;
            this.d = d;
            this.bd = bd;
            this.bool = bool;
            this.bytes = bytes;
            this.date = date;
            this.uuid = uuid;
            this.e1 = e1;
            this.e2 = e2;
        }

        @Id
        String rowKey;      // This will be the row key
        @Column
        String str;
        @Column
        Byte b;
        @Column
        Short s;
        @Column
        Integer i;
        @Column
        Long l;
        @Column
        Float f;
        @Column
        Double d;
        @Column
        BigDecimal bd;
        @Column
        Boolean bool;
        @Column
        byte[] bytes;
        @Column
        Date date;
        @Column
        UUID uuid;
        @Enumerated(EnumType.ORDINAL)
        @Column
        TestEnum e1;
        @Enumerated(EnumType.STRING)
        @Column
        TestEnum e2;

        @Override
        public String toString() {
            return "TestEntityWithTypes ["
                    +   "key="   + rowKey
                    + ", str=" + str
                    + ", b=" + b
                    + ", s=" + s
                    + ", i=" + i
                    + ", l=" + l
                    + ", f=" + f
                    + ", d=" + d
                    + ", bd=" + bd
                    + ", bool=" + bool
                    + ", bytes=" + Arrays.toString(bytes)
                    + ", date=" + date
                    + ", uuid=" + uuid
                    + ", e1=" + e1
                    + ", e2=" + e2 + "]";
        }
    }

    @Test
    public void test() throws Exception {
        Collection<TestEntity> entitiesNative;

        entitiesNative = manager.getAll();
        Assert.assertEquals(48, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);

        entitiesNative = manager.get("A");
        Assert.assertEquals(20, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);

        // Simple row query
        entitiesNative = manager.select()
                .whereId().eq("A")
                .fetch();
        Assert.assertEquals(20, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);


        // Simple prefix
        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .fetch();
        LOG.info("NATIVE: " + entitiesNative);
        Assert.assertEquals(10, entitiesNative.size());

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("b")
                .whereElementId("part2").gte(5L)
                .whereElementId("part2").lt(8L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(3, entitiesNative.size());

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("b")
                .whereElementId("part2").gte(5L)
                .whereElementId("part2").lt(8L)
                .limit(2)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(2, entitiesNative.size());

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("b")
                .whereElementId("part2").gte(5L)
                .whereElementId("part2").lt(8L)
                .whereColumn("value").gte(36L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(2, entitiesNative.size());

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("b")
                .whereElementId("part2").gte(5L)
                .whereElementId("part2").lt(8L)
                .whereColumn("value").gte(36L)
                .limit(1)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(1, entitiesNative.size());

        manager.remove(new TestEntity("A", "b", 5L, null, null));

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("b")
                .whereElementId("part2").gte(5L)
                .whereElementId("part2").lt(8L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(2, entitiesNative.size());

        manager.delete("A");

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("b")
                .whereElementId("part2").gte(5L)
                .whereElementId("part2").lt(8L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(0, entitiesNative.size());

        Collection<TestEntity> entities = Lists.newArrayList();
        entities.add(new TestEntity("B", "a", 5L, null, null));
        entities.add(new TestEntity("B", "b", 5L, null, null));

        manager.remove(entities);

        entitiesNative = manager.select()
                .whereId().eq("B")
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(18, entitiesNative.size());

        entitiesNative = manager.select()
                .whereId().eq("B")
                .limit(4)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(4, entitiesNative.size());

        entitiesNative = manager.get("C");
        Assert.assertEquals(3, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);
    }

    @Test
    public void testIsNull() throws Exception {
        Collection<TestEntity> entitiesNative;

        entitiesNative = manager.select()
                .whereId().eq("E")
                .whereColumn("value2").isNull()
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(1, entitiesNative.size());
    }

    @Test
    public void testIsAbsent() throws Exception {
        Collection<TestEntity> entitiesNative;

        entitiesNative = manager.select()
                .whereId().eq("E")
                .whereColumn("value2").isAbsent()
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(0, entitiesNative.size());
    }

    @Test
    public void testVariableLengthElementId() throws Exception {
        Collection<TestEntity> entitiesNative;

        // Simple row query
        entitiesNative = manager.select()
                .whereId().eq("D")
                .fetch();
        Assert.assertEquals(3, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);


        // Simple prefix
        entitiesNative = manager.select()
                .whereId().eq("D")
                .whereElementId("part1").lte("m")
                .fetch();
        LOG.info("NATIVE: " + entitiesNative);
        Assert.assertEquals(2, entitiesNative.size());

        // Limit
        entitiesNative = manager.select()
                .whereId().eq("D")
                .limit(2)
                .whereElementId("part1").gte("a")
                .fetch();
        LOG.info("NATIVE: " + entitiesNative);
        Assert.assertEquals(2, entitiesNative.size());

        // Verify that "abc...z" not in result, since length affects order
        Iterator<TestEntity> iter = entitiesNative.iterator();
        Assert.assertEquals("m", iter.next().part1);
        Assert.assertEquals("xyz", iter.next().part1);
    }

    @Test
    public void testUpdates() throws Exception {
        Collection<TestEntity> entitiesNative;

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(1L)
                .fetch();
        Assert.assertEquals(1, entitiesNative.size());
        Assert.assertEquals("1", entitiesNative.iterator().next().value2);

        manager.update()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(1L)
                .setColumn("value2", "hi")
                .execute();

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(1L)
                .fetch();
        Assert.assertEquals(1, entitiesNative.size());
        Assert.assertEquals("hi", entitiesNative.iterator().next().value2);

        manager.update()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(1L)
                .setColumn("value2", "bye")
                .ifElementId("part1").eq("a")
                .ifElementId("part2").eq(1L)
                .ifColumn("value2").lte("h")
                .execute();

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(1L)
                .fetch();
        Assert.assertEquals(1, entitiesNative.size());
        Assert.assertEquals("hi", entitiesNative.iterator().next().value2);

        manager.delete()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(1L)
                .ifElementId("part1").eq("a")
                .ifElementId("part2").eq(1L)
                .ifColumn("value2").gte("hi")
                .execute();

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(1L)
                .fetch();
        Assert.assertEquals(0, entitiesNative.size());

        manager.delete()
                .deleteColumn("value2")
                .whereId().eq("C")
                .whereElementId("part1").eq("c")
                .whereElementId("part2").eq(3L)
                .execute();

        entitiesNative = manager.get("C");
        Assert.assertEquals(3, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);
    }

    @Test
    public void testUpdatesIfNull() throws Exception {
        Collection<TestEntity> entitiesNative;

        manager.update()
                .whereId().eq("E")
                .whereElementId("part1").eq("e")
                .whereElementId("part2").eq(4L)
                .setColumn("value2", "blah")
                .ifElementId("part1").eq("e")
                .ifElementId("part2").eq(4L)
                .ifColumn("value2").isNull()
                .execute();

        entitiesNative = manager.select()
                .whereId().eq("E")
                .whereElementId("part1").eq("e")
                .whereElementId("part2").eq(4L)
                .whereColumn("value2").eq("blah")
                .fetch();

        LOG.info("NATIVE: " + entitiesNative);
        Assert.assertEquals(0, entitiesNative.size());

        manager.update()
                .whereId().eq("E")
                .whereElementId("part1").eq("f")
                .whereElementId("part2").eq(5L)
                .setColumn("value2", "bye")
                .ifElementId("part1").eq("f")
                .ifElementId("part2").eq(5L)
                .ifColumn("value2").isNull()
                .execute();

        entitiesNative = manager.select()
                .whereId().eq("E")
                .whereElementId("part1").eq("f")
                .whereElementId("part2").eq(5L)
                .whereColumn("value2").isNull()
                .fetch();

        LOG.info("NATIVE: " + entitiesNative);
        Assert.assertEquals(0, entitiesNative.size());

        manager.update()
                .whereId().eq("E")
                .whereElementId("part1").eq("f")
                .whereElementId("part2").eq(5L)
                .setColumn("value2", null)
                .execute();

        entitiesNative = manager.select()
                .whereId().eq("E")
                .whereElementId("part1").eq("f")
                .whereElementId("part2").eq(5L)
                .whereColumn("value2").isNull()
                .fetch();

        LOG.info("NATIVE: " + entitiesNative);
        Assert.assertEquals(1, entitiesNative.size());
    }

    @Test
    public void testUpdatesIfAbsent() throws Exception {
        Collection<TestEntity> entitiesNative;

        manager.update()
                .whereId().eq("E")
                .whereElementId("part1").eq("e")
                .whereElementId("part2").eq(4L)
                .setColumn("value2", "blah")
                .ifElementId("part1").eq("e")
                .ifElementId("part2").eq(4L)
                .ifColumn("value2").isAbsent()
                .execute();

        entitiesNative = manager.select()
                .whereId().eq("E")
                .whereElementId("part1").eq("e")
                .whereElementId("part2").eq(4L)
                .whereColumn("value2").eq("blah")
                .fetch();

        LOG.info("NATIVE: " + entitiesNative);
        Assert.assertEquals(0, entitiesNative.size());

        manager.update()
                .whereId().eq("E")
                .whereElementId("part1").eq("f")
                .whereElementId("part2").eq(5L)
                .setColumn("value2", "bye")
                .ifElementId("part1").eq("f")
                .ifElementId("part2").eq(5L)
                .ifColumn("value2").isAbsent()
                .execute();

        entitiesNative = manager.select()
                .whereId().eq("E")
                .whereElementId("part1").eq("f")
                .whereElementId("part2").eq(5L)
                .whereColumn("value2").isAbsent()
                .fetch();

        LOG.info("NATIVE: " + entitiesNative);
        Assert.assertEquals(0, entitiesNative.size());

        manager.update()
                .whereId().eq("E")
                .whereElementId("part1").eq("f")
                .whereElementId("part2").eq(5L)
                .setColumn("value2", null)
                .execute();

        entitiesNative = manager.select()
                .whereId().eq("E")
                .whereElementId("part1").eq("f")
                .whereElementId("part2").eq(5L)
                .whereColumn("value2").isAbsent()
                .fetch();

        LOG.info("NATIVE: " + entitiesNative);
        Assert.assertEquals(0, entitiesNative.size());
    }

    @Test
    public void testMutations() throws Exception {
        Collection<TestEntity> entitiesNative;

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(1L)
                .fetch();
        Assert.assertEquals(1, entitiesNative.size());
        Assert.assertEquals("1", entitiesNative.iterator().next().value2);

        UpdateQuery<TestEntity, String> updateQuery = manager.update()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(1L)
                .setColumn("value2", "hi");

        DeleteQuery<TestEntity, String> deleteQuery = manager.delete()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(2L);

        manager.mutate()
                .whereId().eq("A")
                .add(updateQuery, deleteQuery)
                .execute();

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(1L)
                .fetch();
        Assert.assertEquals(1, entitiesNative.size());
        Assert.assertEquals("hi", entitiesNative.iterator().next().value2);

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(2L)
                .fetch();
        Assert.assertEquals(0, entitiesNative.size());


        updateQuery = manager.update()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(1L)
                .setColumn("value2", "bye");

        deleteQuery = manager.delete()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(3L);

        manager.mutate()
                .whereId().eq("A")
                .add(updateQuery, deleteQuery)
                .ifElementId("part1").eq("a")
                .ifElementId("part2").eq(1L)
                .ifColumn("value2").eq("bye")
                .execute();

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(1L)
                .fetch();
        Assert.assertEquals(1, entitiesNative.size());
        Assert.assertEquals("hi", entitiesNative.iterator().next().value2);

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(3L)
                .fetch();
        Assert.assertEquals(1, entitiesNative.size());


        manager.mutate()
                .whereId().eq("A")
                .add(updateQuery, deleteQuery)
                .ifElementId("part1").eq("a")
                .ifElementId("part2").eq(1L)
                .ifColumn("value2").eq("hi")
                .execute();

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(1L)
                .fetch();
        Assert.assertEquals(1, entitiesNative.size());
        Assert.assertEquals("bye", entitiesNative.iterator().next().value2);

        entitiesNative = manager.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .whereElementId("part2").eq(3L)
                .fetch();
        Assert.assertEquals(0, entitiesNative.size());
    }

    @Test
    public void testBadMutations() throws Exception {
        try {
            Collection<TestEntity> entitiesNative;

            entitiesNative = manager.select()
                    .whereId().eq("A")
                    .whereElementId("part1").eq("a")
                    .whereElementId("part2").eq(1L)
                    .fetch();
            Assert.assertEquals(1, entitiesNative.size());
            Assert.assertEquals("1", entitiesNative.iterator().next().value2);

            UpdateQuery<TestEntity, String> updateQuery = manager.update()
                    .whereId().eq("A")
                    .whereElementId("part1").eq("a")
                    .whereElementId("part2").eq(1L)
                    .setColumn("value2", "hi")
                    .ifElementId("part1").eq("a")
                    .ifElementId("part2").eq(1L)
                    .ifColumn("value2").eq("1");

            DeleteQuery<TestEntity, String> deleteQuery = manager.delete()
                    .whereId().eq("A")
                    .whereElementId("part1").eq("a")
                    .whereElementId("part2").eq(1L)
                    .ifElementId("part1").eq("a")
                    .ifElementId("part2").eq(1L)
                    .ifColumn("value2").eq("1");

            manager.mutate()
                    .whereId().eq("A")
                    .add(updateQuery, deleteQuery)
                    .execute();

            Assert.fail();
        }
        catch (Exception e) {
            LOG.info(e.getMessage(), e);
        }
    }

    @Test
    public void testOrdering() throws Exception {
        Collection<TestEntityWithOrdering> entitiesNative;

        entitiesNative = manager2.get("A");
        Assert.assertEquals(20, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);

        // Simple row query
        entitiesNative = manager2.select()
                .whereId().eq("A")
                .fetch();
        Assert.assertEquals(20, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);

        // Simple prefix
        entitiesNative = manager2.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("a")
                .fetch();
        LOG.info("NATIVE: " + entitiesNative);
        Assert.assertEquals(10, entitiesNative.size());

        entitiesNative = manager2.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("b")
                .whereElementId("part2").gte(5L)
                .whereElementId("part2").lt(8L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(3, entitiesNative.size());

        manager2.remove(new TestEntityWithOrdering("A", "b", 5L, null, null));

        entitiesNative = manager2.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("b")
                .whereElementId("part2").gte(5L)
                .whereElementId("part2").lt(8L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(2, entitiesNative.size());

        manager2.delete("A");

        entitiesNative = manager2.select()
                .whereId().eq("A")
                .whereElementId("part1").eq("b")
                .whereElementId("part2").gte(5L)
                .whereElementId("part2").lt(8L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(0, entitiesNative.size());

    }

    @Test
    public void testWithNoElementIds() throws Exception {
        Collection<TestEntityWithNoElementIds> entitiesNative;

        entitiesNative = manager3.get("A");
        Assert.assertEquals(1, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);

        // Simple row query
        entitiesNative = manager3.select()
                .whereId().eq("A")
                .fetch();
        Assert.assertEquals(1, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);
    }


    @Test
    public void testWithNoColumns() throws Exception {
        Collection<TestEntityWithNoColumns> entitiesNative;

        entitiesNative = manager4.get("A");
        Assert.assertEquals(1, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);

        // Simple row query
        entitiesNative = manager4.select()
                .whereId().eq("A")
                .fetch();
        Assert.assertEquals(1, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);
    }

    @Test
    public void testQuery() throws Exception {
        Collection<TestEntity> entitiesNative;

        entitiesNative = manager.select()
                .whereId().eq("B")
                .whereElementId("part1").eq("b")
                .whereElementId("part2").gte(5L)
                .whereElementId("part2").lt(8L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(3, entitiesNative.size());

        entitiesNative = manager.select()
                .whereId().eq("B")
                .whereElementId("part1").eq("b")
                .whereColumn("value").lt(25L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(5, entitiesNative.size());

        entitiesNative = manager.select()
                .whereId().eq("B")
                .whereColumn("value").gte(16L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(12, entitiesNative.size());

        entitiesNative = manager.select()
                .whereId().eq("B")
                .whereColumn("value").gte(16L)
                .whereColumn("value").lte(25L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(4, entitiesNative.size());

        entitiesNative = manager.select()
                .whereId().eq("B")
                .whereColumn("value").neq(16L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(18, entitiesNative.size());

        LongCodec longCodec = new LongCodec();
        entitiesNative = manager.select()
                .whereId().eq("B")
                .whereElementId("part1").eq("a")
                .addColumnPredicate(new BooleanPredicate().setOp(BooleanOp.OR)
                        .addPredicate(new ColumnPredicate().setName("value").setOp(CompareOp.EQUAL).setValue(longCodec.encode(16L)))
                        .addPredicate(new ColumnPredicate().setName("value").setOp(CompareOp.EQUAL).setValue(longCodec.encode(36L))))
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(2, entitiesNative.size());

        int count = manager.select()
                .whereId().eq("B")
                .whereColumn("value").gte(16L)
                .count();

        Assert.assertEquals(12, count);
    }

    @Test
    public void testSelectOneQuery() throws Exception {
        TestEntity entity;

        entity = manager.selectOne()
                .whereId().eq("B")
                .whereElementId("part1").eq("b")
                .whereElementId("part2").eq(5L)
                .fetchOne();

        Assert.assertEquals("b", entity.part1);
        Assert.assertEquals(5L, entity.part2.longValue());
    }

    @Test
    public void testQueryWithColumnFamilies() throws Exception {
        Collection<TestEntityWithColumnFamilies> entitiesNative;
        TestEntityWithColumnFamilies entity;

        entitiesNative = manager5.select()
                .whereId().eq("C")
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(3, entitiesNative.size());

        entitiesNative = manager5.select()
                .fromColumnFamily("c")
                .whereId().eq("C")
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(2, entitiesNative.size());
        entity = entitiesNative.iterator().next();
        Assert.assertEquals("c", entity.family);

        entitiesNative = manager5.select()
                .fromColumnFamily("d")
                .whereId().eq("C")
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(1, entitiesNative.size());
        entity = entitiesNative.iterator().next();
        Assert.assertEquals("d", entity.family);

        entitiesNative = manager5.select()
                .fromColumnFamily("c")
                .whereId().eq("D")
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(0, entitiesNative.size());

        entitiesNative = manager5.select()
                .fromColumnFamily("d")
                .whereId().eq("D")
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(1, entitiesNative.size());
        entity = entitiesNative.iterator().next();
        Assert.assertEquals("d", entity.family);

        manager5.update()
                .fromColumnFamily("c")
                .whereId().eq("C")
                .whereElementId("part1").eq("c1")
                .whereElementId("part2").eq(10L)
                .setColumn("value2", "bye")
                .execute();

        entitiesNative = manager5.select()
                .fromColumnFamily("c")
                .whereId().eq("C")
                .whereElementId("part1").eq("c1")
                .whereElementId("part2").eq(10L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(1, entitiesNative.size());
        entity = entitiesNative.iterator().next();
        Assert.assertEquals("c", entity.family);
        Assert.assertEquals("bye", entity.value2);

        manager5.delete()
                .fromColumnFamily("d")
                .whereId().eq("D")
                .execute();

        entitiesNative = manager5.select()
                .fromColumnFamily("d")
                .whereId().eq("D")
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(0, entitiesNative.size());

        entitiesNative = manager5.select()
                .fromColumnFamily("d")
                .whereId().eq("C")
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(1, entitiesNative.size());
        entity = entitiesNative.iterator().next();
        Assert.assertEquals("d", entity.family);

        manager5.update()
                .fromColumnFamily("d")
                .whereId().eq("C")
                .whereElementId("part1").eq("c3")
                .whereElementId("part2").eq(12L)
                .setColumn("value2", "if")
                .ifColumnFamily("c")
                .ifElementId("part1").eq("c1")
                .ifElementId("part2").eq(10L)
                .ifColumn("value2").eq("xxx")
                .execute();

        entitiesNative = manager5.select()
                .fromColumnFamily("d")
                .whereId().eq("C")
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(1, entitiesNative.size());
        entity = entitiesNative.iterator().next();
        Assert.assertEquals("d", entity.family);
        Assert.assertEquals("102", entity.value2);

        manager5.update()
                .fromColumnFamily("d")
                .whereId().eq("C")
                .whereElementId("part1").eq("c3")
                .whereElementId("part2").eq(12L)
                .setColumn("value2", "if")
                .ifColumnFamily("c")
                .ifElementId("part1").eq("c1")
                .ifElementId("part2").eq(10L)
                .ifColumn("value2").eq("bye")
                .execute();

        entitiesNative = manager5.select()
                .fromColumnFamily("d")
                .whereId().eq("C")
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(1, entitiesNative.size());
        entity = entitiesNative.iterator().next();
        Assert.assertEquals("d", entity.family);
        Assert.assertEquals("if", entity.value2);
    }

    @Test
    public void testBadFieldName() throws Exception {

        try {
            manager.select()
                    .whereId().eq("A")
                    .whereElementId("badfield").eq("b")
                    .fetch();
            Assert.fail();
        }
        catch (Exception e) {
            LOG.info(e.getMessage(), e);
        }
    }

    // Can only test with testMode = false as HMockTable does not support TTL
    //@Test
    public void testMethodTtl() throws Exception {
        manager6.put(new TestEntityWithTTL("A", "foo"));

        Collection<TestEntityWithTTL> entitiesNative;
        entitiesNative = manager6.select()
                .whereId().eq("A")
                .fetch();
        Assert.assertEquals(1, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);

        Thread.sleep(1000 * 4);

        entitiesNative = manager6.select()
                .whereId().eq("A")
                .fetch();
        Assert.assertEquals(0, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);
    }

    @Test
    public void testEntityWithTypes() throws Exception {
        managerTypes.put(new TestEntityWithTypes("A", "s",
                Byte.valueOf((byte)10),
                Short.valueOf((short)11),
                Integer.valueOf(12),
                Long.valueOf(13L),
                Float.valueOf(14.5f),
                Double.valueOf(15.6d),
                BigDecimal.valueOf(16.7d),
                Boolean.TRUE,
                new byte[] { 17, 18 },
                new Date(11111111),
                new UUID(19L, 20L),
                TestEnum.ENUM1,
                TestEnum.ENUM2));

        TestEntityWithTypes entity;
        entity = managerTypes.selectOne()
                .whereId().eq("A")
                .fetchOne();
        LOG.info("NATIVE: " + entity);

        assertEquals("s", entity.str);
        assertEquals(Byte.valueOf((byte)10), entity.b);
        assertEquals(Short.valueOf((short)11), entity.s);
        assertEquals(Integer.valueOf(12), entity.i);
        assertEquals(Long.valueOf(13L), entity.l);
        assertEquals(Float.valueOf(14.5f), entity.f);
        assertEquals(Double.valueOf(15.6d), entity.d);
        assertEquals(BigDecimal.valueOf(16.7d), entity.bd);
        assertEquals(Boolean.TRUE, entity.bool);
        assertThat(Arrays.equals(new byte[] { 17, 18 }, entity.bytes), is(true));
        assertEquals(new Date(11111111), entity.date);
        assertEquals(new UUID(19L, 20L), entity.uuid);
        assertEquals(TestEnum.ENUM1, entity.e1);
        assertEquals(TestEnum.ENUM2, entity.e2);
    }

    @Test
    public void testReferencingCompaction() throws Exception {
        Collection<TestEntityRefing> entitiesNative;

        factory.majorCompactTable(new TableName("testentityrefing"));
        // run a second time as ref cf may have been processed first last time
        factory.majorCompactTable(new TableName("testentityrefing"));

        entitiesNative = managerRefing.get("A");
        Assert.assertEquals(9, entitiesNative.size());

        entitiesNative = managerRefing.select().fromColumnFamily("c").whereId().eq("A").fetch();
        Assert.assertEquals(5, entitiesNative.size());

        entitiesNative = managerRefing.select().fromColumnFamily("ref").whereId().eq("A").fetch();
        Assert.assertEquals(4, entitiesNative.size());

        entitiesNative = managerRefing.get("B");
        Assert.assertEquals(9, entitiesNative.size());

        entitiesNative = managerRefing.select().fromColumnFamily("c").whereId().eq("B").fetch();
        Assert.assertEquals(5, entitiesNative.size());

        entitiesNative = managerRefing.select().fromColumnFamily("ref").whereId().eq("B").fetch();
        Assert.assertEquals(4, entitiesNative.size());
    }

    @Test
    public void testIndexingCompaction() throws Exception {
        Collection<TestEntityIndexing> entitiesNative;

        factory.majorCompactTable(new TableName("testentityindexing"));
        // run a second time as index cf may have been processed first last time
        factory.majorCompactTable(new TableName("testentityindexing"));

        entitiesNative = managerIndexing.get("A");
        Assert.assertEquals(9, entitiesNative.size());

        entitiesNative = managerIndexing.select().fromColumnFamily("c").whereId().eq("A").fetch();
        Assert.assertEquals(5, entitiesNative.size());

        entitiesNative = managerIndexing.select().fromColumnFamily("ix").whereId().eq("A").fetch();
        Assert.assertEquals(4, entitiesNative.size());

        entitiesNative = managerIndexing.get("B");
        Assert.assertEquals(9, entitiesNative.size());

        entitiesNative = managerIndexing.select().fromColumnFamily("c").whereId().eq("B").fetch();
        Assert.assertEquals(5, entitiesNative.size());

        entitiesNative = managerIndexing.select().fromColumnFamily("ix").whereId().eq("B").fetch();
        Assert.assertEquals(4, entitiesNative.size());
    }

    @Test
    public void entityTrimmingCompaction() throws Exception {
        final int NUM_ELEMENT_IDS = 3;
        byte[] key = new byte[]{1, 1};
        CompositeBuilder composite = new CompositeBuilder(64, CompareOp.EQUAL);
        for (int i = 0; i < NUM_ELEMENT_IDS; i++) {
            composite.addWithoutControl(ByteBuffer.wrap(new StringCodec().encode("foo")));
        }
        ByteBuffer bb = composite.get();
        byte[] fooBytes = new byte[bb.remaining()];
        bb.get(fooBytes);

        composite = new CompositeBuilder(64, CompareOp.EQUAL);
        for (int i = 0; i < NUM_ELEMENT_IDS; i++) {
            composite.addWithoutControl(ByteBuffer.wrap(new StringCodec().encode("bar")));
        }
        bb = composite.get();
        byte[] barBytes = new byte[bb.remaining()];
        bb.get(barBytes);

        composite = new CompositeBuilder(64, CompareOp.EQUAL);
        for (int i = 0; i < NUM_ELEMENT_IDS; i++) {
            composite.addWithoutControl(ByteBuffer.wrap(new StringCodec().encode("zap")));
        }
        bb = composite.get();
        byte[] zapBytes = new byte[bb.remaining()];
        bb.get(zapBytes);

        EntityTrimmingCompactionFilter<String> filter = new EntityTrimmingCompactionFilter<>();
        Map<String, String> config = ImmutableMap.of(
                CompactionFilter.HENTITYDB_PREFIX + ".cf." + EntityTrimmingCompactionFilter.MAX_ENTITIES,
                String.valueOf(2),
                CompactionFilter.HENTITYDB_PREFIX + ".cf." + EntityTrimmingCompactionFilter.NUM_ELEMENT_IDS,
                String.valueOf(NUM_ELEMENT_IDS));
        TestColumn testColumn = new TestColumn(fooBytes, null);
        TestColumn testColumn2 = new TestColumn(barBytes, null);
        TestColumn testColumn3 = new TestColumn(zapBytes, null);
        filter.setup(config, new TableName("yay"), "cf", "myregion");
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn)),
                is(true));
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn2)),
                is(true));
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn3)),
                is(false));

        filter = new EntityTrimmingCompactionFilter<>();
        config = ImmutableMap.of(
                CompactionFilter.HENTITYDB_PREFIX + ".cf." + EntityTrimmingCompactionFilter.MAX_ENTITIES,
                String.valueOf(2),
                CompactionFilter.HENTITYDB_PREFIX + ".cf." + EntityTrimmingCompactionFilter.MAX_ENTITIES_TTL,
                String.valueOf(2),
                CompactionFilter.HENTITYDB_PREFIX + ".cf." + EntityTrimmingCompactionFilter.NUM_ELEMENT_IDS,
                String.valueOf(NUM_ELEMENT_IDS));
        testColumn = new TestColumn(fooBytes, null, System.currentTimeMillis() - 2*1000);
        testColumn2 = new TestColumn(barBytes, null, System.currentTimeMillis() - 2*1000);
        testColumn3 = new TestColumn(zapBytes, null, System.currentTimeMillis() - 2*1000);
        filter.setup(config, new TableName("yay"), "cf", "myregion");
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn)),
                is(true));
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn2)),
                is(true));
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn3)),
                is(false));

        filter = new EntityTrimmingCompactionFilter<>();
        config = ImmutableMap.of(
                CompactionFilter.HENTITYDB_PREFIX + ".cf." + EntityTrimmingCompactionFilter.MAX_ENTITIES,
                String.valueOf(2),
                CompactionFilter.HENTITYDB_PREFIX + ".cf." + EntityTrimmingCompactionFilter.MAX_ENTITIES_TTL,
                String.valueOf(2),
                CompactionFilter.HENTITYDB_PREFIX + ".cf." + EntityTrimmingCompactionFilter.NUM_ELEMENT_IDS,
                String.valueOf(NUM_ELEMENT_IDS));
        testColumn = new TestColumn(fooBytes, null, System.currentTimeMillis());
        testColumn2 = new TestColumn(barBytes, null, System.currentTimeMillis());
        testColumn3 = new TestColumn(zapBytes, null, System.currentTimeMillis());
        filter.setup(config, new TableName("yay"), "cf", "myregion");
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn)),
                is(true));
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn2)),
                is(true));
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn3)),
                is(true));
    }

    @Test
    public void testQueryInbox() throws Exception {
        managerInbox.put(new PInboxEntry("o", "inbox1", 10L, 1L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("o", "inbox1", 20L, 2L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("o", "inbox1", 30L, 3L, 0, false, false, false, false, 0L));

        Collection<PInboxEntry> entitiesNative;
        PInboxEntry entity;

        entitiesNative = managerInbox.select()
                .whereId().eq("inbox1")
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(3, entitiesNative.size());

        entitiesNative = managerInbox.select()
                .whereId().eq("inbox1")
                .whereElementId("elementId").lt(25L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(2, entitiesNative.size());
    }

    @Test
    public void testUpdateInbox() throws Exception {
        managerInbox.put(new PInboxEntry("o", "inbox1", 10L, 1L, 0, false, false, false, false, null));
        managerInbox.put(new PInboxEntry("o", "inbox1", 20L, 2L, 0, false, false, false, false, null));
        managerInbox.put(new PInboxEntry("o", "inbox1", 30L, 3L, 0, false, false, false, false, null));

        Collection<PInboxEntry> entitiesNative;
        PInboxEntry entity;

        entitiesNative = managerInbox.select()
                .whereId().eq("inbox1")
                .whereElementId("elementId").eq(10L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(1, entitiesNative.size());

        managerInbox.update()
                .fromColumnFamily("o")
                .whereId().eq("inbox1")
                .whereElementId("elementId").eq(10L)
                .setColumn("valueId", 15L)
                .setColumn("lastReadMessageId", null)
                .execute();

        entitiesNative = managerInbox.select()
                .whereId().eq("inbox1")
                .whereElementId("elementId").eq(10L)
                .fetch();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(1, entitiesNative.size());

        Assert.assertEquals(null, entitiesNative.iterator().next().getLastReadMessageId());
    }

    @Test
    public void testLongSearchInbox() {
        long tsec = 1461111617;

        managerInbox.put(new PInboxEntry("o", "inboxByteCmp", 10L, 1L, 0, false, false, false, false, tsec - 3600*3));
        managerInbox.put(new PInboxEntry("o", "inboxByteCmp", 20L, 2L, 0, false, false, false, false, tsec - 3600*2));
        managerInbox.put(new PInboxEntry("o", "inboxByteCmp", 30L, 3L, 0, false, false, false, false, tsec - 3600));

        long startSec = tsec - 3600*2 -1;
        List res = managerInbox.select().whereId().eq("inboxByteCmp")
                .whereElementId("elementId").gt(0L)
                .whereColumn("lastReadMessageId").gt(startSec).fetch();

        assertThat(res.size(), is(2));

        startSec = tsec - 7000;
        res = managerInbox.select().whereId().eq("inboxByteCmp")
                .whereElementId("elementId").gt(0L)
                .whereColumn("lastReadMessageId").gt(startSec).fetch();

        assertThat(res.size(), is(1));
    }

    @Test
    public void testInboxCompaction() throws Exception {
        managerInbox.put(new PInboxEntry("o", "inbox1", 10L, 1L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("o", "inbox1", 20L, 2L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("o", "inbox1", 30L, 3L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("o", "inbox1", 40L, 4L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("o", "inbox1", 50L, 5L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("o", "inbox1", 60L, 6L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("i", "inbox1", 1L, 10L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("i", "inbox1", 2L, 20L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("i", "inbox1", 3L, 30L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("i", "inbox1", 4L, 40L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("i", "inbox1", 5L, 50L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("i", "inbox1", 6L, 60L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("u", "inbox1", 10L, 1L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("u", "inbox1", 20L, 2L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("u", "inbox1", 30L, 3L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("u", "inbox1", 40L, 4L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("u", "inbox1", 50L, 5L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("u", "inbox1", 60L, 6L, 0, false, false, false, false, 0L));

        Collection<PInboxEntry> entitiesNative;
        PInboxEntry entity;

        entitiesNative = managerInbox.get("inbox1");
        Assert.assertEquals(18, entitiesNative.size());

        // Note that we only return 5 entities here even though there are 6 in the table
        entitiesNative = managerInbox.select().fromColumnFamily("o").whereId().eq("inbox1").fetch();
        Assert.assertEquals(5, entitiesNative.size());

        factory.majorCompactTable(new TableName("test:pinboxes"));
        // run a second time as index cf may have been processed first last time
        factory.majorCompactTable(new TableName("test:pinboxes"));

        entitiesNative = managerInbox.get("inbox1");
        Assert.assertEquals(15, entitiesNative.size());

        entitiesNative = managerInbox.select().fromColumnFamily("o").whereId().eq("inbox1").fetch();
        Assert.assertEquals(5, entitiesNative.size());

        entitiesNative = managerInbox.select().fromColumnFamily("i").whereId().eq("inbox1").fetch();
        Assert.assertEquals(5, entitiesNative.size());

        entitiesNative = managerInbox.select().fromColumnFamily("u").whereId().eq("inbox1").fetch();
        Assert.assertEquals(5, entitiesNative.size());
    }

    @Test
    public void testCount() {
        managerInbox.put(new PInboxEntry("o", "inbox1", 10L, 1L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("o", "inbox1", 20L, 2L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("o", "inbox1", 30L, 3L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("o", "inbox1", 40L, 4L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("o", "inbox1", 50L, 5L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("o", "inbox1", 60L, 6L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("i", "inbox1", 1L, 10L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("i", "inbox1", 2L, 20L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("i", "inbox1", 3L, 30L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("i", "inbox1", 4L, 40L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("i", "inbox1", 5L, 50L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("i", "inbox1", 6L, 60L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("u", "inbox1", 10L, 1L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("u", "inbox1", 20L, 2L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("u", "inbox1", 30L, 3L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("u", "inbox1", 40L, 4L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("u", "inbox1", 50L, 5L, 0, false, false, false, false, 0L));
        managerInbox.put(new PInboxEntry("u", "inbox1", 60L, 6L, 0, false, false, false, false, 0L));

        int count = managerInbox.select()
                .whereId().eq("inbox1")
                .whereElementId("elementId").eq(10L)
                .count();

        Assert.assertEquals(2, count);

        count = managerInbox.select()
                .fromColumnFamily("u")
                .whereId().eq("inbox1")
                .whereElementId("elementId").eq(10L)
                .count();

        Assert.assertEquals(1, count);

        count = managerInbox.select()
                .fromColumnFamily("u")
                .whereId().eq("inbox1")
                .count();

        Assert.assertEquals(6, count);
    }
}
