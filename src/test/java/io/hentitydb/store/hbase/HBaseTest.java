package io.hentitydb.store.hbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.hentitydb.Environment;
import io.hentitydb.EntityConfiguration;
import io.hentitydb.serialization.*;
import io.hentitydb.store.*;
import org.apache.hadoop.conf.Configuration;
import org.junit.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@SuppressWarnings("ConstantConditions")
public class HBaseTest extends ColumnStoreTest {

    protected TableName inboxTableName;
    protected TableName inboxRefTableName;
    protected TableName inboxIdxTableName;
    protected Table<String, Long> inboxTable;
    protected Table<String, Long> inboxRefTable;
    protected Table<String, Long> inboxIdxTable;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        inboxTableName = new TableName("test:inbox");
        TableMetadata<String, Long> inboxTableMetadata = new TableMetadata<>(
                inboxTableName, "cf",  new StringCodec(), new LongCodec(), 5, null, null);
        factory.declareTable(inboxTableMetadata);
        this.inboxTable = conn.getTable(inboxTableName);

        inboxTable.put("my").addColumn(1L, 1L).addColumn(2L, 2L).execute();

        inboxRefTableName = new TableName("test:inboxref");
        TableMetadata<String, Long> inboxRefTableMetadata = new TableMetadata<>(
                inboxRefTableName, "cf",  new StringCodec(), new LongCodec(), 5, "ref", null,
                null, null);
        ColumnFamilyMetadata<String, Long> referencingColumnMetadata = new ColumnFamilyMetadata<>(
                "ref", null, null);
        inboxRefTableMetadata.addColumnFamily(referencingColumnMetadata);
        factory.declareTable(inboxRefTableMetadata);
        this.inboxRefTable = conn.getTable(inboxRefTableName);

        inboxRefTable.put("my").addColumn(1L, 1L).addColumn(2L, 2L).execute();

        inboxIdxTableName = new TableName("test:inboxidx");
        TableMetadata<String, Long> inboxIdxTableMetadata = new TableMetadata<>(
                inboxIdxTableName, "o",  new StringCodec(), new InvertedLongCodec(), 5, null, "i",
                ImmutableList.of(new VersionedVarLongCodec(), new VarLongCodec()), null);
        ColumnFamilyMetadata<String, Long> indexingColumnMetadata = new ColumnFamilyMetadata<>(
                "i", null, null);
        inboxIdxTableMetadata.addColumnFamily(indexingColumnMetadata);
        factory.declareTable(inboxIdxTableMetadata);
        this.inboxIdxTable = conn.getTable(inboxIdxTableName);

        inboxRefTable.put("my").addColumn(1L, 1L).addColumn(2L, 2L).execute();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();

        inboxTable.truncate();
        inboxRefTable.truncate();
        inboxIdxTable.truncate();
    }

    @BeforeClass
    public static void setUpTests() throws Exception {
        Configuration hconfig = new Configuration();
        hconfig.set("hbase.zookeeper.quorum", "127.0.0.1");
        hconfig.set("zookeeper.znode.parent", "/hbase-unsecure");

        config = new EntityConfiguration(hconfig);
        config.setAutoTableCreation(true);
        config.setNamespacePrefix("testprefix_");
        config.setTestMode(true);
        factory = Environment.getConnectionFactory(config);
        conn = factory.createConnection();
    }

    @Test
    public void testReferencingCompaction() throws Exception {
        inboxRefTable.put("my").addColumn(1L, 1L).addColumn(2L, 2L).addColumn(3L, 3L).addColumn(4L, 4L)
                .addColumn(5L, 5L).addColumn(6L, 6L).addColumn(7L, 7L).execute();
        inboxRefTable.put("my").addColumn("ref", 2L, 2L).addColumn("ref", 3L, 3L).addColumn("ref", 4L, 4L)
                .addColumn("ref", 5L, 5L).addColumn("ref", 6L, 6L).addColumn("ref", 7L, 7L).execute();

        factory.majorCompactTable(inboxRefTableName);
        // run a second time as ref cf may have been processed first last time
        factory.majorCompactTable(inboxRefTableName);

        Row<String, Long> row = inboxRefTable.get("my").addFamily("cf").execute();
        assertThat(row.getColumns().size(),
                is(5));

        row = inboxRefTable.get("my").addFamily("ref").execute();
        assertThat(row.getColumns().size(),
                is(4));
    }

    @Test
    public void testIndexingCompactionWithPartialCodec() throws Exception {
        Message thread1 = new Message(1, 100);
        Message thread2 = new Message(2, 200);
        Message thread3 = new Message(3, 300);
        Message thread4 = new Message(4, 400);
        Message thread5 = new Message(5, 500);
        Message thread6 = new Message(6, 600);
        Message thread7 = new Message(7, 700);
        inboxIdxTable.put("5:1:1")
                .addColumn(1L, thread1, MessageCodec.INSTANCE)
                .addColumn(2L, thread2, MessageCodec.INSTANCE)
                .addColumn(3L, thread3, MessageCodec.INSTANCE)
                .addColumn(4L, thread4, MessageCodec.INSTANCE)
                .addColumn(5L, thread5, MessageCodec.INSTANCE)
                .addColumn(6L, thread6, MessageCodec.INSTANCE)
                .addColumn(7L, thread7, MessageCodec.INSTANCE).execute();

        inboxIdxTable.put("5:1:1")
                .addColumn("i", 100L, thread1, MessageCodec.INSTANCE)
                .addColumn("i", 200L, thread2, MessageCodec.INSTANCE)
                .addColumn("i", 300L, thread3, MessageCodec.INSTANCE)
                .addColumn("i", 400L, thread4, MessageCodec.INSTANCE)
                .addColumn("i", 500L, thread5, MessageCodec.INSTANCE)
                .addColumn("i", 600L, thread6, MessageCodec.INSTANCE)
                .addColumn("i", 700L, thread7, MessageCodec.INSTANCE).execute();

        factory.majorCompactTable(inboxIdxTableName);
        // run a second time as index cf may have been processed first last time
        factory.majorCompactTable(inboxIdxTableName);

        Row<String, Long> row = inboxIdxTable.get("5:1:1").addFamily("o").execute();
        assertThat(row.getColumns().size(),
                is(5));

        row = inboxIdxTable.get("5:1:1").addFamily("i").execute();
        assertThat(row.getColumns().size(),
                is(5));
    }

    @AfterClass
    public static void cleanUpTests() {
        try {
            conn.close();
        } catch (Exception e) {
            // ignore
        }
    }
}
