package io.hentitydb.store;

import java.util.Iterator;

public interface RowScanner<K, C> extends AutoCloseable, Iterable<Row<K, C>>, Iterator<Row<K, C>> {
}
