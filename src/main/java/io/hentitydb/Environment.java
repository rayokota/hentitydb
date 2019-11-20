package io.hentitydb;

import com.google.common.base.Throwables;
import io.hentitydb.entity.DefaultEntityContext;
import io.hentitydb.entity.EntityContext;
import io.hentitydb.store.Connection;
import io.hentitydb.store.ConnectionFactory;

public class Environment {
    @SuppressWarnings("unchecked")
    public static ConnectionFactory getConnectionFactory(EntityConfiguration config) {
        if (config == null) config = new EntityConfiguration();

        try {
            String clsName = "io.hentitydb.store.hbase.HBaseConnectionFactory";
            Class<? extends ConnectionFactory> cls = (Class<? extends ConnectionFactory>)Class.forName(clsName);
            return cls.getConstructor(EntityConfiguration.class)
                    .newInstance(config);

        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T, K> EntityContext<T, K> getEntityContext(Connection connection, Class<T> entityType) {
        return new DefaultEntityContext<>(connection, entityType);
    }
}
