package io.hentitydb.entity.hbase;

import com.google.common.collect.ImmutableMap;
import io.hentitydb.Configuration;
import io.hentitydb.entity.EntityContextTest;
import io.hentitydb.Environment;
import org.junit.AfterClass;
import org.junit.BeforeClass;

@SuppressWarnings("ConstantConditions")
public class HBaseEntityContextTest extends EntityContextTest {

    @BeforeClass
    public static void setUpTests() throws Exception {
        config = new Configuration();
        config.setJarFilePath("/usr/local/hbase/jars/hentitydb.jar");
        config.setAutoTableCreation(true);
        config.setNamespacePrefix("testprefix_");
        config.setTestMode(true);

        config.setProperties(ImmutableMap.of(
                "hbase.zookeeper.quorum", "127.0.0.1",
                "zookeeper.znode.parent", "/hbase-unsecure"
        ));

        factory = Environment.getConnectionFactory(config);
        conn = factory.createConnection();
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
