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

import java.util.List;
import java.util.RandomAccess;

public final class Iterables {

    /**
     * Removes, from an iterable, every element that satisfies the provided
     * predicate.
     *
     * <p>Removals may or may not happen immediately as each element is tested
     * against the predicate.  The behavior of this method is not specified if
     * {@code predicate} is dependent on {@code removeFrom}.
     *
     * @param removeFrom the iterable to (potentially) remove elements from
     * @param predicate a predicate that determines whether an element should
     *     be removed
     * @return {@code true} if any elements were removed from the iterable
     *
     * @throws UnsupportedOperationException if the iterable does not support
     *     {@code remove()}.
     * @since 2.0
     */
    public static <T> boolean removeIf(Iterable<T> removeFrom, IterablePredicate<? super T> predicate) {
        if (removeFrom instanceof RandomAccess && removeFrom instanceof List) {
            return removeIfFromRandomAccessList((List<T>) removeFrom, checkNotNull(predicate));
        }
        return Iterators.removeIf(removeFrom.iterator(), predicate);
    }

    private static <T> boolean removeIfFromRandomAccessList(
            List<T> list, IterablePredicate<? super T> predicate) {
        // Note: Not all random access lists support set(). Additionally, it's possible
        // for a list to reject setting an element, such as when the list does not permit
        // duplicate elements. For both of those cases,  we need to fall back to a slower
        // implementation.
        int from = 0;
        int to = 0;

        for (; from < list.size(); from++) {
            T element = list.get(from);
            if (!predicate.apply(from, element)) {
                if (from > to) {
                    try {
                        list.set(to, element);
                    } catch (UnsupportedOperationException | IllegalArgumentException e) {
                        slowRemoveIfForRemainingElements(list, predicate, to, from);
                        return true;
                    }
                }
                to++;
            }
        }

        // Clear the tail of any remaining items
        list.subList(to, list.size()).clear();
        return from != to;
    }

    private static <T> void slowRemoveIfForRemainingElements(
            List<T> list, IterablePredicate<? super T> predicate, int to, int from) {
        // Here we know that:
        // * (to < from) and that both are valid indices.
        // * Everything with (index < to) should be kept.
        // * Everything with (to <= index < from) should be removed.
        // * The element with (index == from) should be kept.
        // * Everything with (index > from) has not been checked yet.

        // Check from the end of the list backwards (minimize expected cost of
        // moving elements when remove() is called). Stop before 'from' because
        // we already know that should be kept.
        for (int n = list.size() - 1; n > from; n--) {
            if (predicate.apply(n, list.get(n))) {
                list.remove(n);
            }
        }
        // And now remove everything in the range [to, from) (going backwards).
        for (int n = from - 1; n >= to; n--) {
            list.remove(n);
        }
    }
}
