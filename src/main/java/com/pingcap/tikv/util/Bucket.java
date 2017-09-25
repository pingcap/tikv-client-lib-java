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

import javax.annotation.ParametersAreNonnullByDefault;

public class Bucket implements Comparable<Bucket> {
  public long count;
  public long repeats;
  public Comparable lowerBound;
  public Comparable upperBound;

  public Bucket(long count, long repeats, Comparable lowerBound, Comparable upperBound) {
    this.count = count;
    this.repeats = repeats;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    assert upperBound != null;
  }

  /** used for binary search only */
  public Bucket(Comparable upperBound) {
    this.upperBound = upperBound;
    assert upperBound != null;
  }

  @Override
  @ParametersAreNonnullByDefault
  public int compareTo(Bucket b) {
    if(upperBound instanceof Comparables.ComparableBytes && b.upperBound instanceof Comparables.ComparableBytes) {
      return ((Comparables.ComparableBytes) upperBound).compareTo(((Comparables.ComparableBytes) b.upperBound));
    } else if(upperBound instanceof Comparables.ComparableByteString && b.upperBound instanceof Comparables.ComparableByteString) {
      return ((Comparables.ComparableByteString) upperBound).compareTo(((Comparables.ComparableByteString) b.upperBound));
    } else {
      return upperBound.compareTo(b.upperBound);
    }
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

  public Comparable getLowerBound() {
    return lowerBound;
  }

  public void setLowerBound(Comparable lowerBound) {
    this.lowerBound = lowerBound;
  }

  public Comparable getUpperBound() {
    return upperBound;
  }

  public void setUpperBound(Comparable upperBound) {
    this.upperBound = upperBound;
  }

  @Override
  public String toString() {
    return "{count=" + count + ", repeats=" + repeats + ", range=[" + lowerBound + ", "
        + upperBound.toString() + "]}";
  }

}