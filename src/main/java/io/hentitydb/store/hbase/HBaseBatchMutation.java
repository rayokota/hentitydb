package io.hentitydb.store.hbase;

import com.google.common.collect.Lists;
import io.hentitydb.store.BatchMutation;
import io.hentitydb.store.RowMutation;

import java.util.List;

public class HBaseBatchMutation<K, C> implements BatchMutation<K, C> {

    private final HBaseTable<K, C> table;
    private final List<RowMutation<K, C>> mutations = Lists.newLinkedList();

    public HBaseBatchMutation(HBaseTable<K, C> table) {
        this.table = table;
    }

    @Override
    public final HBaseBatchMutation<K, C> add(RowMutation<K, C> rowMutation) {
        mutations.add(rowMutation);
        return this;
    }

    @Override
    public final HBaseBatchMutation<K, C> add(List<RowMutation<K, C>> rowMutations) {
        mutations.addAll(rowMutations);
        return this;
    }

    @Override
    public void execute() {
        table.doBatchMutations(mutations);
    }
}
