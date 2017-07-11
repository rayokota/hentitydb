package io.hentitydb.store.hbase;

import io.hentitydb.store.HealthCheck;
import io.hentitydb.store.TableName;
import org.apache.hadoop.hbase.client.Admin;

import java.util.concurrent.*;

public class HBaseHealthCheck implements HealthCheck {

    private final TableName tableName;
    private final HBaseConnection connection;
    private final ExecutorService executor;

    public HBaseHealthCheck(HBaseConnectionFactory factory, TableName tableName) {
        this.tableName = tableName;
        this.connection = factory.createConnection();
        this.executor = Executors.newSingleThreadExecutor();
    }

    @Override
    public String getName() {
        return "hbase";
    }

    @Override
    public Result check() {
        try {
            Future<Boolean> future = executor.submit(this::doCheck);
            boolean isHealthy = future.get(5000, TimeUnit.MILLISECONDS);
            return new Result(isHealthy);
        } catch (Exception e) {
            return new Result(false, "HBase is not reachable");
        }
    }

    private boolean doCheck() {
        try (Admin hadmin = connection.getHConnection().getAdmin()) {
            org.apache.hadoop.hbase.TableName name = connection.getConnectionFactory().toHTableName(tableName);
            return hadmin.isTableAvailable(name);
        } catch (Exception e) {
            return false;
        }
    }
}
