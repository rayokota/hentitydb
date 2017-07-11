package io.hentitydb.store;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.hentitydb.Configuration;
import io.hentitydb.serialization.SaltingCodec;
import io.hentitydb.serialization.StringCodec;
import org.junit.*;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

@SuppressWarnings("ConstantConditions")
public abstract class ColumnStoreTest {
    protected static Configuration config;
    protected static ConnectionFactory factory;
    protected static Connection conn;
    protected Table<String, String> table;
    protected Table<String, String> saltedTable;
    protected Table<String, String> counterTable;
    protected Table<String, String> multiColumnTable;

    @Before
    public void setUp() throws Exception {
        TableName tableName = new TableName("yay");
        TableMetadata<String, String> metadata = new TableMetadata(tableName, "cf", new StringCodec(), new StringCodec());
        factory.declareTable(metadata);
        this.table = conn.getTable(tableName);

        table.put("1").addColumn("value", "one").execute();
        table.put("2").addColumn("value", "two").execute();
        table.put("3").addColumn("value", "three").execute();
        table.put("4").addColumn("value", "four").execute();
        table.put("5").addColumn("value", "five").execute();
        table.put("6").addColumn("value", "six").execute();
        table.put("7").addColumn("value", "seven").execute();
        table.put("8").addColumn("value", "eight").execute();
        table.put("9").addColumn("value", "nine").execute();

        tableName = new TableName("salt");
        metadata = new TableMetadata(tableName, "cf", new SaltingCodec(new StringCodec()), new StringCodec());
        factory.declareTable(metadata);
        this.saltedTable = conn.getTable(tableName);

        saltedTable.put("1").addColumn("value", "one").execute();
        saltedTable.put("2").addColumn("value", "two").execute();
        saltedTable.put("3").addColumn("value", "three").execute();
        saltedTable.put("4").addColumn("value", "four").execute();
        saltedTable.put("5").addColumn("value", "five").execute();
        saltedTable.put("6").addColumn("value", "six").execute();
        saltedTable.put("7").addColumn("value", "seven").execute();
        saltedTable.put("8").addColumn("value", "eight").execute();
        saltedTable.put("9").addColumn("value", "nine").execute();

        tableName = new TableName("yip");
        TableMetadata<String, String> counterTableMetadata = new TableMetadata(tableName, "cf", new StringCodec(), new StringCodec(),
                null, null, ImmutableMap.of("default_validation_class", "CounterColumnType"));
        factory.declareTable(counterTableMetadata);
        this.counterTable = conn.getTable(tableName);

        tableName = new TableName("yup");
        TableMetadata<String, String> multiColumnTableMetadata = new TableMetadata(tableName, "cf", new StringCodec(), new StringCodec());
        factory.declareTable(multiColumnTableMetadata);
        this.multiColumnTable = conn.getTable(tableName);

        multiColumnTable.put("abc").addColumn("value", 1L).addColumn("value2", true).execute();
        multiColumnTable.put("fgh").addColumn("value", 3L).execute();
    }

    @After
    public void tearDown() throws Exception {
        table.truncate();
        saltedTable.truncate();
        counterTable.truncate();
        multiColumnTable.truncate();

        table.close();
        saltedTable.close();
        counterTable.close();
        multiColumnTable.close();
    }

    @Test
    public void readingANonExistentValue() throws Exception {
        assertThat(table.get("foo").addColumn("value").execute().getString("value"),
                is(nullValue()));
    }

    @Test
    public void readingAnExistingValue() throws Exception {
        table.put("foo").addColumn("value", "wah").execute();
        assertThat(table.get("foo").addColumn("value").execute().getString("value"),
                is("wah"));
    }

    @Test
    public void readingTypedValues() throws Exception {
        table.put("foo")
                .addColumn("boolean", true)
                .addColumn("short", (short) 3)
                .addColumn("int", 4)
                .addColumn("long", 5L)
                .addColumn("date", new Date(1000000L))
                .addColumn("float", (float) 6.7)
                .addColumn("double", 8.9)
                .addColumn("bytes", new byte[]{0x01, 0x02, 0x03, 0x04})
                .addColumn("value", "hello", new StringCodec())
                .execute();


        Row<String, String> row = table.get("foo").execute();
        assertThat(row.getColumns().size(),
                is(9));
        assertThat(row.getBoolean("boolean"),
                is(true));
        assertThat(row.getShort("short"),
                is((short)3));
        assertThat(row.getInt("int"),
                is(4));
        assertThat(row.getLong("long"),
                is(5L));
        assertThat(row.getDate("date"),
                is(new Date(1000000L)));
        assertThat(row.getFloat("float"),
                is((float)6.7));
        assertThat(row.getDouble("double"),
                is(8.9));
        assertThat(row.getBytes("bytes"),
                is(new byte[]{0x01, 0x02, 0x03, 0x04}));
        assertThat(row.getValue("value", new StringCodec()),
                is("hello"));
    }

    @Test
    public void readingAllColumnsForExistingValue() throws Exception {
        table.put("foo").addColumn("value", "wah").execute();
        assertThat(table.get("foo").addAll().execute().getString("value"),
                is("wah"));
    }

    @Test
    public void updatingANonExistentValue() throws Exception {
        table.put("foo").addColumn("value", "wah").executeIfAbsent("value");
        assertThat(table.get("foo").addColumn("value").execute().getString("value"),
                is("wah"));
    }

    @Test
    public void updatingAnExistingValue() throws Exception {
        table.put("foo").addColumn("value", "wah").execute();
        table.put("foo").addColumn("value", "woo").executeIf("value", CompareOp.EQUAL, "wah");
        assertThat(table.get("foo").addColumn("value").execute().getString("value"),
                is("woo"));
    }

    @Test
    public void deleteANonExistentValue() throws Exception {
        table.delete("foo").addColumn("value").executeIfAbsent("value");
        assertThat(table.get("foo").addColumn("value").execute().getString("value"),
                is(nullValue()));
    }

    @Test
    public void deleteAnExistingValue() throws Exception {
        table.put("foo").addColumn("value", "wah").execute();
        table.delete("foo").addColumn("value").executeIf("value", CompareOp.EQUAL, "wah");
        assertThat(table.get("foo").addColumn("value").execute().getString("value"),
                is(nullValue()));
    }

    @Test
    public void deleteAnExistingRow() throws Exception {
        table.put("foo").addColumn("value", "wah").execute();
        table.delete("foo").addAll().execute();
        assertThat(table.get("foo").execute().isEmpty(),
                is(true));
    }

    @Test
    public void addingAValue() throws Exception {
        assertThat(table.get("foo").addColumn("value").execute().getString("value"),
                is(nullValue()));
        table.put("foo").addColumn("value", "wah").execute();
        assertThat(table.get("foo").addColumn("value").execute().getString("value"),
                is("wah"));
    }

    @Test
    public void overwritingAValue() throws Exception {
        table.put("foo").addColumn("value", "woo").execute();
        assertThat(table.get("foo").addColumn("value").execute().getString("value"),
                is("woo"));
        table.put("foo").addColumn("value", "wah").execute();
        assertThat(table.get("foo").addColumn("value").execute().getString("value"),
                is("wah"));
    }

    @Test
    public void removeEntries() throws Exception {
        table.delete("one").execute();

        assertThat(table.get("one").addColumn("value").execute().getString("value"),
                is(nullValue()));
    }

    @Test
    public void incrementAValue() throws Exception {
        counterTable.increment("abc").addColumn("value", 3L).execute();
        assertThat(counterTable.get("abc").addColumn("value").execute().getLong("value"),
                is(3L));

        counterTable.increment("abc").addColumn("value", 4L).execute();
        assertThat(counterTable.get("abc").addColumn("value").execute().getLong("value"),
                is(7L));
    }

    @Test
    public void batchOperations() throws Exception {
        table.put("foo").addColumn("value", "wah").execute();
        table.put("bar").addColumn("value", "woo").execute();
        assertThat(table.get("foo").addColumn("value").execute().getString("value"),
                is("wah"));
        BatchOperation<String, String> batch = table.batchOperations();
        batch.add(table.get("foo").addColumn("value"));
        batch.add(table.get("bar").addColumn("value"));
        Object[] rows = batch.execute();

        assertThat(((Row<String, String>) rows[0]).getString("value"),
                is("wah"));
        assertThat(((Row<String, String>)rows[1]).getString("value"),
                is("woo"));
    }

    @Test
    public void batchMutations() throws Exception {
        Put<String, String> put = multiColumnTable.put("abc").addColumn("value", 5L);
        Delete<String, String> delete = multiColumnTable.delete("abc").addColumn("value2");
        Put<String, String> put2 = multiColumnTable.put("fgh").addColumn("value", 7L);
        multiColumnTable.batchMutations().add(put).add(delete).add(put2).execute();

        assertThat(multiColumnTable.get("abc").addColumn("value").execute().getLong("value"),
                is(5L));
        assertThat(multiColumnTable.get("abc").addColumn("value2").execute().getString("value2"),
                is(nullValue()));
        assertThat(multiColumnTable.get("fgh").addColumn("value").execute().getLong("value"),
                is(7L));
    }

    @Test
    public void atomicRowMutations() throws Exception {
        Put<String, String> put = multiColumnTable.put("abc").addColumn("value", 5L);
        Delete<String, String> delete = multiColumnTable.delete("abc").addColumn("value2");
        Put<String, String> put2 = multiColumnTable.put("abc").addColumn("value3", 7L);
        multiColumnTable.mutateRow("abc").add(put, delete, put2).execute();

        assertThat(multiColumnTable.get("abc").addColumn("value").execute().getLong("value"),
                is(5L));
        assertThat(multiColumnTable.get("abc").addColumn("value2").execute().getString("value2"),
                is(nullValue()));
        assertThat(multiColumnTable.get("abc").addColumn("value3").execute().getLong("value3"),
                is(7L));
    }

    @Test
    public void atomicRowMutationsWithCheck() throws Exception {
        Put<String, String> put = multiColumnTable.put("abc").addColumn("value", 5L);
        Delete<String, String> delete = multiColumnTable.delete("abc").addColumn("value2");
        Put<String, String> put2 = multiColumnTable.put("abc").addColumn("value3", 7L);
        multiColumnTable.mutateRow("abc").add(put, delete, put2).executeIf("value", CompareOp.EQUAL, 0L);

        assertThat(multiColumnTable.get("abc").addColumn("value").execute().getLong("value"),
                is(1L));
        assertThat(multiColumnTable.get("abc").addColumn("value2").execute().getBoolean("value2"),
                is(true));
        assertThat(multiColumnTable.get("abc").addColumn("value3").execute().getString("value3"),
                is(nullValue()));

        multiColumnTable.mutateRow("abc").add(put, delete, put2).executeIf("value", CompareOp.EQUAL, 1L);

        assertThat(multiColumnTable.get("abc").addColumn("value").execute().getLong("value"),
                is(5L));
        assertThat(multiColumnTable.get("abc").addColumn("value2").execute().getString("value2"),
                is(nullValue()));
        assertThat(multiColumnTable.get("abc").addColumn("value3").execute().getLong("value3"),
                is(7L));
    }

    @Test
    public void readingAColumnRange() throws Exception {
        multiColumnTable.put("ijk")
                .addColumn("1", 1L).addColumn("2", 2L).addColumn("3", 3L).addColumn("40", 4L).addColumn("5", 5L)
                .addColumn("6", 6L).addColumn("7", 7L).addColumn("8", 8L).addColumn("9", 9L).addColumn("10", 10L).execute();

        Row<String, String> row = multiColumnTable.get("ijk").withColumnRange("3", "5").execute();
        assertThat(row.getColumns().size(),
                is(3));
        assertThat(row.isNull("1"),
                is(true));
        assertThat(row.isNull("2"),
                is(true));
        assertThat(row.getLong("3"),
                is(3L));
        assertThat(row.getLong("40"),
                is(4L));
        assertThat(row.getLong("5"),
                is(5L));
        assertThat(row.isNull("6"),
                is(true));
    }

    @Test
    public void readingAColumnRangeWithLimit() throws Exception {
        multiColumnTable.put("ijk")
                .addColumn("1", 1L).addColumn("2", 2L).addColumn("3", 3L).addColumn("4", 4L).addColumn("5", 5L)
                .addColumn("6", 6L).addColumn("7", 7L).addColumn("8", 8L).addColumn("9", 9L).addColumn("10", 10L).execute();

        Row<String, String> row = multiColumnTable.get("ijk").withColumnRange("3", "5", 2).execute();
        assertThat(row.getColumns().size(),
                is(2));
        assertThat(row.isNull("1"),
                is(true));
        assertThat(row.isNull("2"),
                is(true));
        assertThat(row.getLong("3"),
                is(3L));
        assertThat(row.getLong("4"),
                is(4L));
        assertThat(row.isNull("5"),
                is(true));
        assertThat(row.isNull("6"),
                is(true));
    }

    @Test
    public void readingAColumnRangeWithZeroLimit() throws Exception {
        multiColumnTable.put("ijk")
                .addColumn("1", 1L).addColumn("2", 2L).addColumn("3", 3L).addColumn("4", 4L).addColumn("5", 5L)
                .addColumn("6", 6L).addColumn("7", 7L).addColumn("8", 8L).addColumn("9", 9L).addColumn("10", 10L).execute();

        Row<String, String> row = multiColumnTable.get("ijk").withColumnRange("3", "5", 0).execute();
        assertThat(row.isEmpty(),
                is(true));
    }

    @Test
    public void readingAnExclusiveColumnRangeWithLimit() throws Exception {
        multiColumnTable.put("ijk")
                .addColumn("1", 1L).addColumn("2", 2L).addColumn("3", 3L).addColumn("4", 4L).addColumn("5", 5L)
                .addColumn("6", 6L).addColumn("7", 7L).addColumn("8", 8L).addColumn("9", 9L).addColumn("10", 10L).execute();

        Row<String, String> row = multiColumnTable.get("ijk").withColumnRange("3", false, "5", false, 3).execute();
        assertThat(row.getColumns().size(),
                is(1));
        assertThat(row.isNull("1"),
                is(true));
        assertThat(row.isNull("2"),
                is(true));
        assertThat(row.isNull("3"),
                is(true));
        assertThat(row.getLong("4"),
                is(4L));
        assertThat(row.isNull("5"),
                is(true));
        assertThat(row.isNull("6"),
                is(true));
    }

    @Test
    public void readingAnInvalidColumnRange() throws Exception {
        multiColumnTable.put("ijk")
                .addColumn("1", 1L).addColumn("2", 2L).addColumn("3", 3L).addColumn("4", 4L).addColumn("5", 5L)
                .addColumn("6", 6L).addColumn("7", 7L).addColumn("8", 8L).addColumn("9", 9L).addColumn("10", 10L).execute();

        Row<String, String> row = multiColumnTable.get("ijk").withColumnRange("5", "3").execute();
        assertThat(row.isEmpty(),
                is(true));
    }

    @Test
    public void filterAndColumnRange() throws Exception {
        if (!config.getTestMode()) return;
        multiColumnTable.put("ijk")
                .addColumn("1", 1L).addColumn("2", 2L).addColumn("3", 3L).addColumn("4", 4L).addColumn("5", 5L)
                .addColumn("6", 6L).addColumn("7", 7L).addColumn("8", 8L).addColumn("9", 9L).addColumn("10", 10L).execute();

        Row<String, String> row = multiColumnTable.get("ijk").withColumnRange("3", "5")
                .addFilter(new TestColumnFilter("4"))
                .execute();
        assertThat(row.getColumns().size(),
                is(2));
        assertThat(row.isNull("1"),
                is(true));
        assertThat(row.isNull("2"),
                is(true));
        assertThat(row.getLong("3"),
                is(3L));
        assertThat(row.isNull("4"),
                is(true));
        assertThat(row.getLong("5"),
                is(5L));
        assertThat(row.isNull("6"),
                is(true));
    }

    @Test
    public void filterOrColumnRange() throws Exception {
        multiColumnTable.put("ijk")
                .addColumn("1", 1L).addColumn("2", 2L).addColumn("3", 3L).addColumn("4", 4L).addColumn("5", 5L)
                .addColumn("6", 6L).addColumn("7", 7L).addColumn("8", 8L).addColumn("9", 9L).addColumn("10", 10L).execute();

        Row<String, String> row = multiColumnTable.get("ijk").withColumnRange("3", "5")
                .addFilter(new ColumnCountGetFilter<>(1))
                .setFilterOp(BooleanOp.OR)
                .execute();
        assertThat(row.getColumns().size(),
                is(4));
        assertThat(row.getLong("1"),
                is(1L));
        assertThat(row.isNull("2"),
                is(true));
        assertThat(row.getLong("3"),
                is(3L));
        assertThat(row.getLong("4"),
                is(4L));
        assertThat(row.getLong("5"),
                is(5L));
        assertThat(row.isNull("6"),
                is(true));
    }

    @Test
    public void filterOrFilteredColumnRange() throws Exception {
        if (!config.getTestMode()) return;
        multiColumnTable.put("ijk")
                .addColumn("1", 1L).addColumn("2", 2L).addColumn("3", 3L).addColumn("4", 4L).addColumn("5", 5L)
                .addColumn("6", 6L).addColumn("7", 7L).addColumn("8", 8L).addColumn("9", 9L).addColumn("10", 10L).execute();

        Row<String, String> row = multiColumnTable.get("ijk").withColumnRange(
                "3", true, "5", true, Integer.MAX_VALUE, new TestColumnFilter("4"))
                .addFilter(new ColumnCountGetFilter<>(1))
                .setFilterOp(BooleanOp.OR)
                .execute();
        assertThat(row.getColumns().size(),
                is(3));
        assertThat(row.getLong("1"),
                is(1L));
        assertThat(row.isNull("2"),
                is(true));
        assertThat(row.getLong("3"),
                is(3L));
        assertThat(row.isNull("4"),
                is(true));
        assertThat(row.getLong("5"),
                is(5L));
        assertThat(row.isNull("6"),
                is(true));
    }

    @Test
    public void scansRangesAtTheBeginningOfTheDatabase() throws Exception {
        Map<String, String> entries = Maps.newHashMap();
        try (RowScanner<String, String> scanner = table.scan("1", "3")) {
            for (Row<String, String> result : scanner) {
                entries.put(result.getKey(), result.getString("value"));

            }
        }
        ImmutableMap<String, String> result = ImmutableMap.copyOf(entries);

        assertThat(result,
                   is(ImmutableMap.of(
                           "1", "one",
                           "2", "two",
                           "3", "three"
                   )));
    }

    @Test
    public void scansRangesAtTheEndOfTheDatabase() throws Exception {
        Map<String, String> entries = Maps.newHashMap();
        try (RowScanner<String, String> scanner = table.scan("7", "9")) {
            for (Row<String, String> result : scanner) {
                entries.put(result.getKey(), result.getString("value"));

            }
        }
        ImmutableMap<String, String> result = ImmutableMap.copyOf(entries);

        assertThat(result,
                is(ImmutableMap.of(
                        "7", "seven",
                        "8", "eight",
                        "9", "nine"
                )));
    }

    @Test
    public void scansRangesInTheMiddleOfTheDatabase() throws Exception {
        Map<String, String> entries = Maps.newHashMap();
        try (RowScanner<String, String> scanner = table.scan("3", "6")) {
            for (Row<String, String> result : scanner) {
                entries.put(result.getKey(), result.getString("value"));

            }
        }
        ImmutableMap<String, String> result = ImmutableMap.copyOf(entries);

        assertThat(result,
                is(ImmutableMap.of(
                        "3", "three",
                        "4", "four",
                        "5", "five",
                        "6", "six"
                )));
    }

    @Test
    public void scansTheFullDatabase() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = table.scan("1", "9")) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }
        ImmutableList<Map.Entry<String, String>> result = ImmutableList.copyOf(entries);

        assertThat(result,
                is(ImmutableList.of(
                        Maps.immutableEntry("1", "one"),
                        Maps.immutableEntry("2", "two"),
                        Maps.immutableEntry("3", "three"),
                        Maps.immutableEntry("4", "four"),
                        Maps.immutableEntry("5", "five"),
                        Maps.immutableEntry("6", "six"),
                        Maps.immutableEntry("7", "seven"),
                        Maps.immutableEntry("8", "eight"),
                        Maps.immutableEntry("9", "nine")
                )));
    }

    @Test
    public void scansRangesBeforeTheBeginningOfTheDatabase() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = table.scan("+", "0")) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }

        assertThat(entries.isEmpty(),
                   is(true));
    }

    @Test
    public void scansRangesAfterTheEndOfTheDatabase() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = table.scan("A", "Z")) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }

        assertThat(entries.isEmpty(),
                is(true));
    }

    @Test
    public void scansRangesOverlappingTheEndOfTheDatabase() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = table.scan("8", "A")) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }
        ImmutableList<Map.Entry<String, String>> result = ImmutableList.copyOf(entries);

        assertThat(result,
                is(ImmutableList.of(
                        Maps.immutableEntry("8", "eight"),
                        Maps.immutableEntry("9", "nine")
                )));
    }

    @Test
    public void scansRangesOverlappingTheBeginningOfTheDatabase() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = table.scan("0", "2")) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }
        ImmutableList<Map.Entry<String, String>> result = ImmutableList.copyOf(entries);

        assertThat(result,
                is(ImmutableList.of(
                        Maps.immutableEntry("1", "one"),
                        Maps.immutableEntry("2", "two")
                )));
    }

    @Test
    public void reverseScansRangesAtTheBeginningOfTheDatabase() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = table.reverseScan("3", "1")) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }
        ImmutableList<Map.Entry<String, String>> result = ImmutableList.copyOf(entries);

        assertThat(result,
                is(ImmutableList.of(
                        Maps.immutableEntry("3", "three"),
                        Maps.immutableEntry("2", "two"),
                        Maps.immutableEntry("1", "one")
                )));
    }

    @Test
    public void reverseScansRangesAtTheEndOfTheDatabase() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = table.reverseScan("9", "7")) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }
        ImmutableList<Map.Entry<String, String>> result = ImmutableList.copyOf(entries);

        assertThat(result,
                is(ImmutableList.of(
                        Maps.immutableEntry("9", "nine"),
                        Maps.immutableEntry("8", "eight"),
                        Maps.immutableEntry("7", "seven")
                )));
    }

    @Test
    public void reverseScansRangesInTheMiddleOfTheDatabase() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = table.reverseScan("6", "3")) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }
        ImmutableList<Map.Entry<String, String>> result = ImmutableList.copyOf(entries);

        assertThat(result,
                is(ImmutableList.of(
                        Maps.immutableEntry("6", "six"),
                        Maps.immutableEntry("5", "five"),
                        Maps.immutableEntry("4", "four"),
                        Maps.immutableEntry("3", "three")
                )));
    }

    @Test
    public void reverseScansTheFullDatabase() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = table.reverseScan("9", "1")) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }
        ImmutableList<Map.Entry<String, String>> result = ImmutableList.copyOf(entries);

        assertThat(result,
                is(ImmutableList.of(
                        Maps.immutableEntry("9", "nine"),
                        Maps.immutableEntry("8", "eight"),
                        Maps.immutableEntry("7", "seven"),
                        Maps.immutableEntry("6", "six"),
                        Maps.immutableEntry("5", "five"),
                        Maps.immutableEntry("4", "four"),
                        Maps.immutableEntry("3", "three"),
                        Maps.immutableEntry("2", "two"),
                        Maps.immutableEntry("1", "one")
                )));
    }

    @Test
    public void reverseScansRangesBeforeTheBeginningOfTheDatabase() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = table.reverseScan("0", "+")) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }

        assertThat(entries.isEmpty(),
                is(true));
    }

    @Test
    public void reverseScansRangesAfterTheEndOfTheDatabase() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = table.reverseScan("Z", "A")) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }

        assertThat(entries.isEmpty(),
                is(true));
    }

    @Test
    public void reverseScansRangesOverlappingTheEndOfTheDatabase() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = table.reverseScan("A", "8")) {
            for (Row<String, String> result: scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }
        ImmutableList<Map.Entry<String, String>> result = ImmutableList.copyOf(entries);

        assertThat(result,
                   is(ImmutableList.of(
                           Maps.immutableEntry("9", "nine"),
                           Maps.immutableEntry("8", "eight")
                   )));
    }

    @Test
    public void reverseScansRangesOverlappingTheBeginningOfTheDatabase() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = table.reverseScan("2", "0")) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }
        ImmutableList<Map.Entry<String, String>> result = ImmutableList.copyOf(entries);

        assertThat(result,
                is(ImmutableList.of(
                        Maps.immutableEntry("2", "two"),
                        Maps.immutableEntry("1", "one")
                )));
    }

    @Test
    public void scansSaltedRanges() throws Exception {
        Map<String, String> entries = Maps.newHashMap();
        try (RowScanner<String, String> scanner = saltedTable.scan("3", "6")) {
            for (Row<String, String> result : scanner) {
                entries.put(result.getKey(), result.getString("value"));

            }
        }
        ImmutableMap<String, String> result = ImmutableMap.copyOf(entries);

        assertThat(result,
                is(ImmutableMap.of(
                        "3", "three",
                        "4", "four",
                        "5", "five",
                        "6", "six"
                )));
    }

    @Test
    public void scansSaltedRangesInParallel() throws Exception {
        Map<String, String> entries = Maps.newHashMap();
        try (RowScanner<String, String> scanner = saltedTable.scan("3", "6", Executors.newCachedThreadPool())) {
            for (Row<String, String> result : scanner) {
                entries.put(result.getKey(), result.getString("value"));

            }
        }
        ImmutableMap<String, String> result = ImmutableMap.copyOf(entries);

        assertThat(result,
                is(ImmutableMap.of(
                        "3", "three",
                        "4", "four",
                        "5", "five",
                        "6", "six"
                )));
    }

    @Test
    public void reverseScansSaltedRanges() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = saltedTable.reverseScan("6", "3")) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }
        ImmutableList<Map.Entry<String, String>> result = ImmutableList.copyOf(entries);

        assertThat(result,
                is(ImmutableList.of(
                        Maps.immutableEntry("6", "six"),
                        Maps.immutableEntry("5", "five"),
                        Maps.immutableEntry("4", "four"),
                        Maps.immutableEntry("3", "three")
                )));
    }

    @Test
    public void reverseScansSaltedRangesInParallel() throws Exception {
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = saltedTable.reverseScan("6", "3", Executors.newCachedThreadPool())) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }
        ImmutableList<Map.Entry<String, String>> result = ImmutableList.copyOf(entries);

        assertThat(result,
                is(ImmutableList.of(
                        Maps.immutableEntry("6", "six"),
                        Maps.immutableEntry("5", "five"),
                        Maps.immutableEntry("4", "four"),
                        Maps.immutableEntry("3", "three")
                )));
    }

    @Test
    public void filteringAScan() throws Exception {
        if (!config.getTestMode()) return;
        Map<String, String> entries = Maps.newHashMap();
        try (RowScanner<String, String> scanner = table.scan("3", "6", new TestRowFilter("4"))) {
            for (Row<String, String> result : scanner) {
                entries.put(result.getKey(), result.getString("value"));

            }
        }
        ImmutableMap<String, String> result = ImmutableMap.copyOf(entries);

        assertThat(result,
                is(ImmutableMap.of(
                        "3", "three",
                        "5", "five",
                        "6", "six"
                )));
    }

    @Test
    public void filteringAReverseScan() throws Exception {
        if (!config.getTestMode()) return;
        List<Map.Entry<String, String>> entries = Lists.newArrayList();
        try (RowScanner<String, String> scanner = table.reverseScan("6", "3", new TestRowFilter("4"))) {
            for (Row<String, String> result : scanner) {
                entries.add(Maps.immutableEntry(result.getKey(), result.getString("value")));

            }
        }
        ImmutableList<Map.Entry<String, String>> result = ImmutableList.copyOf(entries);

        assertThat(result,
                is(ImmutableList.of(
                        Maps.immutableEntry("6", "six"),
                        Maps.immutableEntry("5", "five"),
                        Maps.immutableEntry("3", "three")
                )));
    }

    @Test
    public void trimmingCompaction() throws Exception {
        byte[] key = new byte[]{1, 1};

        TrimmingCompactionFilter<String, byte[]> filter = new TrimmingCompactionFilter<>();
        Map<String, String> config = ImmutableMap.of(
                CompactionFilter.HENTITYDB_PREFIX + ".cf." + TrimmingCompactionFilter.MAX_COLUMNS,
                String.valueOf(2));
        TestColumn<byte[], byte[]> testColumn = new TestColumn(null, null);
        filter.setup(config, new TableName("yay"), "cf", "myregion");
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn)),
                is(true));
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn)),
                is(true));
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn)),
                is(false));

        filter = new TrimmingCompactionFilter<>();
        config = ImmutableMap.of(
                CompactionFilter.HENTITYDB_PREFIX + ".cf." + TrimmingCompactionFilter.MAX_COLUMNS,
                String.valueOf(2),
                CompactionFilter.HENTITYDB_PREFIX + ".cf." + TrimmingCompactionFilter.MAX_COLUMNS_TTL,
                String.valueOf(2));
        testColumn = new TestColumn(null, null, System.currentTimeMillis() - 2*1000);
        filter.setup(config, new TableName("yay"), "cf", "myregion");
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn)),
                is(true));
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn)),
                is(true));
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn)),
                is(false));

        filter = new TrimmingCompactionFilter<>();
        config = ImmutableMap.of(
                CompactionFilter.HENTITYDB_PREFIX + ".cf." + TrimmingCompactionFilter.MAX_COLUMNS,
                String.valueOf(2),
                CompactionFilter.HENTITYDB_PREFIX + ".cf." + TrimmingCompactionFilter.MAX_COLUMNS_TTL,
                String.valueOf(2));
        testColumn = new TestColumn(null, null, System.currentTimeMillis());
        filter.setup(config, new TableName("yay"), "cf", "myregion");
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn)),
                is(true));
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn)),
                is(true));
        assertThat(filter.filterKeyColumn(new KeyColumn<>(null, key, testColumn)),
                is(true));
    }
}
