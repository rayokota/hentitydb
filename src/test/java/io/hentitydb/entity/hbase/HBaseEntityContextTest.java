package io.hentitydb.entity.hbase;

import com.google.common.collect.ImmutableMap;
import io.hentitydb.EntityConfiguration;
import io.hentitydb.entity.EntityContextTest;
import io.hentitydb.Environment;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

@SuppressWarnings("ConstantConditions")
public class HBaseEntityContextTest extends EntityContextTest {

    @BeforeClass
    public static void setUpTests() throws Exception {
        Configuration hconfig = new Configuration();
        hconfig.set("hbase.zookeeper.quorum", "127.0.0.1");
        hconfig.set("zookeeper.znode.parent", "/hbase-unsecure");

        config = new EntityConfiguration(hconfig);
        config.setJarFilePath("/usr/local/hbase/jars/hentitydb.jar");
        config.setAutoTableCreation(true);
        config.setNamespacePrefix("testprefix_");
        config.setTestMode(true);
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
