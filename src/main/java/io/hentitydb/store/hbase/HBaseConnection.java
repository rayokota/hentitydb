package io.hentitydb.store.hbase;

import com.google.common.base.Throwables;
import io.hentitydb.EntityConfiguration;
import io.hentitydb.store.Connection;
import io.hentitydb.store.TableMetadata;
import io.hentitydb.store.TableName;
import io.hentitydb.store.hbase.security.AuthUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.mock.MockHTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static io.hentitydb.store.hbase.HBaseConnectionFactory.HBASE_CLIENT_JAAS_FILE;
import static io.hentitydb.store.hbase.HBaseConnectionFactory.HBASE_CLIENT_KERBEROS_PRINCIPAL;
import static io.hentitydb.store.hbase.HBaseConnectionFactory.HBASE_CLIENT_KEYTAB_FILE;

public class HBaseConnection implements Connection {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseConnection.class);

    private final HBaseConnectionFactory factory;
    private final org.apache.hadoop.hbase.client.Connection hconnection;

    public HBaseConnection(HBaseConnectionFactory factory) {
        this(factory, null);
    }

    public HBaseConnection(final HBaseConnectionFactory factory, final ExecutorService pool) {
        try {
            this.factory = factory;

            EntityConfiguration config = factory.getConfiguration();
            final Configuration hconfig = config.getHConfiguration();

            UserGroupInformation ugi = null;
            if (factory.isSecure()) {
                // don't require principal and keytab as this code is also used by oozie
                String principal = hconfig.get(HBASE_CLIENT_KERBEROS_PRINCIPAL);
                if (principal == null) {
                    LOG.warn(HBASE_CLIENT_KERBEROS_PRINCIPAL + " not specified");
                }
                String keytab = hconfig.get(HBASE_CLIENT_KEYTAB_FILE);
                if (keytab == null) {
                    LOG.warn(HBASE_CLIENT_KEYTAB_FILE + " not specified");
                }
                // The JAAS config may be needed depending on how ZK is configured
                String jaas = hconfig.get(HBASE_CLIENT_JAAS_FILE);
                if (jaas != null) {
                    System.setProperty("java.security.auth.login.config", jaas);
                }
                if (principal != null && keytab != null) {
                    UserGroupInformation.setConfiguration(hconfig);
                    ugi = AuthUtil.login(principal, keytab);
                    factory.scheduleRelogin(ugi);
                }
            }

            if (config.getTestMode()) {
                this.hconnection = null;
            } else if (ugi == null) {
                this.hconnection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hconfig, pool);
            } else {
                this.hconnection = ugi.doAs((PrivilegedExceptionAction<org.apache.hadoop.hbase.client.Connection>) () -> ConnectionFactory.createConnection(hconfig, pool));
            }

        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public HBaseConnectionFactory getConnectionFactory() {
        return factory;
    }

    public org.apache.hadoop.hbase.client.Connection getHConnection() {
        return hconnection;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, C> HBaseTable<K, C> getTable(TableName tableName) {
        try {
            TableMetadata<K, C> tableMetadata = (TableMetadata<K, C>)factory.getTableMetadata(tableName);
            return new HBaseTable<>(this, tableMetadata);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static final Map<TableName, Table> mockTables = new ConcurrentHashMap<>();

    protected Table getHTable(TableMetadata<?, ?> metadata) throws IOException {
        TableName name = metadata.getTableName();
        org.apache.hadoop.hbase.TableName tableName = factory.toHTableName(name);
        Table table;
        if (getConnectionFactory().getConfiguration().getTestMode()) {
            MockHTable newTable = new MockHTable(tableName, metadata.getColumnFamilyNames());
            table = mockTables.putIfAbsent(name, newTable);
            if (table == null) {
                table = newTable;
            }
        } else {
            table = getHConnection().getTable(tableName);
        }
        return table;
    }

    @Override
    public void close() {
        try {
            if (hconnection != null) hconnection.close();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
