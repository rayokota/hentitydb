package io.hentitydb.store.hbase;

import com.google.common.collect.Lists;
import io.hentitydb.store.BatchOperation;
import io.hentitydb.store.RowOperation;

import java.util.List;

public class HBaseBatchOperation<K, C> implements BatchOperation<K, C> {

    private final HBaseTable<K, C> table;
    private final List<RowOperation<K, C>> operations = Lists.newLinkedList();

    public HBaseBatchOperation(HBaseTable<K, C> table) {
        this.table = table;
    }

    @Override
    public final HBaseBatchOperation<K, C> add(RowOperation<K, C> rowOperation) {
        operations.add(rowOperation);
        return this;
    }

    @Override
    public final HBaseBatchOperation<K, C> add(List<RowOperation<K, C>> rowOperations) {
        operations.addAll(rowOperations);
        return this;
    }

    @Override
    public Object[] execute() {
        return table.doBatchOperations(operations);
    }
}
