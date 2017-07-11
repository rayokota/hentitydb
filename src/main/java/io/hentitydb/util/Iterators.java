/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hentitydb.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

public final class Iterators {

    /**
     * Removes every element that satisfies the provided predicate from the
     * iterator. The iterator will be left exhausted: its {@code hasNext()}
     * method will return {@code false}.
     *
     * @param removeFrom the iterator to (potentially) remove elements from
     * @param predicate a predicate that determines whether an element should
     *     be removed
     * @return {@code true} if any elements were removed from the iterator
     * @since 2.0
     */
    public static <T> boolean removeIf(Iterator<T> removeFrom, IterablePredicate<? super T> predicate) {
        checkNotNull(predicate);
        boolean modified = false;
        int n = 0;
        while (removeFrom.hasNext()) {
            if (predicate.apply(n, removeFrom.next())) {
                removeFrom.remove();
                modified = true;
            }
            n++;
        }
        return modified;
    }
}
