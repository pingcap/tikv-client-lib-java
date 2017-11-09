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

import com.pingcap.tikv.meta.TiKey;

public class Bucket implements Comparable<Bucket> {
  public long count;
  public long repeats;
  public TiKey lowerBound;
  public TiKey upperBound;

  public Bucket(long count, long repeats, Object lowerBound, Object upperBound) {
    this.count = count;
    this.repeats = repeats;
    this.lowerBound = TiKey.encode(lowerBound);
    this.upperBound = TiKey.encode(upperBound);
  }

  public Bucket(long count, long repeats, TiKey lowerBound, TiKey upperBound) {
    this.count = count;
    this.repeats = repeats;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    assert upperBound != null;
  }

  /** used for binary search only */
  public Bucket(TiKey upperBound) {
    this.upperBound = upperBound;
    assert upperBound != null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public int compareTo(Bucket b) {
    return upperBound.compareTo(b.upperBound);
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public long getRepeats() {
    return repeats;
  }

  public void setRepeats(long repeats) {
    this.repeats = repeats;
  }

  public TiKey getLowerBound() {
    return lowerBound;
  }

  public void setLowerBound(TiKey lowerBound) {
    this.lowerBound = lowerBound;
  }

  public TiKey getUpperBound() {
    return upperBound;
  }

  public void setUpperBound(TiKey upperBound) {
    this.upperBound = upperBound;
  }

  @Override
  public String toString() {
    return "{count=" + count + ", repeats=" + repeats + ", range=[" + lowerBound + ", "
        + upperBound.toString() + "]}";
  }

}