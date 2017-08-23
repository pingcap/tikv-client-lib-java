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
  }

  public Bucket() {
  }

  @Override
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

}