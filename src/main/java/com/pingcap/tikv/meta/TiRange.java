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

package com.pingcap.tikv.meta;
import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.Comparator;

public class TiRange<E> implements Serializable {
    private final E lowVal;
    private final E highVal;
    private final boolean leftOpen;
    private final boolean rightOpen;
    // This is almost a hack for tiSpark
    // In reality comparator needs to be deseraialized as well
    // but in case of TiSpark, we don't do any compare anymore
    transient private Comparator<E> comparator;

    public static <T> TiRange<T> create(T l, T h, boolean lopen, boolean ropen, Comparator<T> c) {
        return new TiRange(l, h, lopen, ropen, c);
    }

    public static <T extends Comparable<T>> TiRange<T> create(T l, T h) {
        return new TiRange(l, h, false, true, Comparator.naturalOrder());
    }

    public static <T> TiRange<T> create(T l, T h, Comparator<T> comp) {
        return new TiRange(l, h, false, true, comp);
    }

    public static TiRange<ByteString> createByteStringRange(ByteString l, ByteString h) {
        return new TiRange<>(l, h, false, true, Comparator.comparing(ByteString::asReadOnlyByteBuffer));
    }

    public static TiRange<ByteString> createByteStringRange(ByteString l, ByteString h, boolean lopen, boolean ropen) {
        return new TiRange<>(l, h, lopen, ropen, Comparator.comparing(ByteString::asReadOnlyByteBuffer));
    }

    private TiRange(E l, E h, boolean lopen, boolean ropen, Comparator<E> comp) {
        this.lowVal = l;
        this.highVal = h;
        this.leftOpen = lopen;
        this.rightOpen = ropen;
        this.comparator = comp;
    }

    public E getLowValue() {
        return lowVal;
    }

    public E getHighValue() {
        return highVal;
    }

    public boolean contains(E v) {
        return compare(v) == 0;
    }

    public boolean isLeftOpen() {
        return leftOpen;
    }

    public boolean isRightOpen() {
        return rightOpen;
    }

    public Comparator<E> getComparator() {
        return comparator;
    }

    public int compare(E v) {
        if (!isLeftOpen()  && comparator.compare(getLowValue(), v) == 0 ||
            !isRightOpen() && comparator.compare(getHighValue(), v) == 0) {
            return 0;
        }
        if (comparator.compare(getLowValue(), v) >= 0) {
            return -1;
        }
        if (comparator.compare(getHighValue(), v) <= 0) {
            return 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        String lowStr = getLowValue().toString();
        String highStr = getHighValue().toString();
        if (lowVal.getClass().equals(ByteString.class)) {
            lowStr = ((ByteString)lowVal).toStringUtf8();
            highStr = ((ByteString)highVal).toStringUtf8();
        }
        return String.format("%s%s,%s%s", isLeftOpen()? "(" : "[",
                                          lowStr, highStr,
                                          isRightOpen() ? ")" : "]");
    }
}
