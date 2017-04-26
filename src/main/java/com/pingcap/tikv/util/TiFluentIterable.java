/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.util;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkNotNull;

// Similar to Guava's FluentIterable but over both iterator
// and iterables. In addition, provide stopWhen interface
public class TiFluentIterable<E> implements Iterable<E> {
    private final Iterable<E> iter;

    private TiFluentIterable(Iterable<E> iter) {
        this.iter = iter;
    }

    public static <E> TiFluentIterable<E> from(Iterable<E> iter) {
        checkNotNull(iter);
        return (iter instanceof TiFluentIterable) ?
               (TiFluentIterable<E>)iter : new TiFluentIterable<>(iter);
    }

    public static <E> TiFluentIterable<E> from(Iterator<E> iter) {
        checkNotNull(iter);
        return from(new IteratorIterable(iter));
    }

    public TiFluentIterable<E> stopWhen(Predicate<? super E> pred) {
        return from(UntilIterable.decorate(iter, pred));
    }

    public <T> TiFluentIterable<T> transform(Function<? super E, T> function) {
        return from(Iterables.transform(iter, function));
    }

    public final TiFluentIterable<E> filter(Predicate<? super E> predicate) {
        return from(Iterables.filter(iter, predicate));
    }

    public final E[] toArray(Class<E> type) {
        return Iterables.toArray(iter, type);
    }

    @Override
    public Iterator<E> iterator() {
        return iter.iterator();
    }

    // Iterator is not rewindable so that we need a copy of values
    private static class IteratorIterable<E> implements Iterable<E> {
        private List<E> rewindBuf;

        private IteratorIterable(Iterator<E> iter) {
            this.rewindBuf = ImmutableList.copyOf(iter);
        }

        @Override
        public Iterator<E> iterator() {
            return rewindBuf.iterator();
        }
    }

    private static class UntilIterable<E> implements Iterable<E> {
        private final Predicate<? super E> pred;
        private final Iterable<E>  iter;

        public static <E> UntilIterable<E> decorate(Iterable<E> iter, Predicate<? super E> pred) {
            return new UntilIterable(iter, pred);
        }

        private UntilIterable(Iterable<E> iter, Predicate<? super E> pred) {
            this.pred = pred;
            this.iter = iter;
        }

        @Override
        public Iterator<E> iterator() {
            return new UntilIterator();
        }

        private class UntilIterator implements Iterator<E> {
            private final Iterator<E> iterator;

            private boolean more = true;
            private E       peeked;

            private UntilIterator() {
                this.iterator = iter.iterator();
            }

            @Override
            public boolean hasNext() {
                if (!iterator.hasNext()) {
                    return false;
                }
                return maybeReadAndCheck();
            }

            private boolean maybeReadAndCheck() {
                if (peeked == null) {
                    peeked = iterator.next();
                    if (pred.apply(peeked)) {
                        more = false;
                    }
                }
                return more;
            }

            @Override
            public E next() {
                if (!more) {
                    throw new NoSuchElementException();
                }
                maybeReadAndCheck();

                E value = peeked;
                peeked = null;
                return value;
            }
        }
    }
}
