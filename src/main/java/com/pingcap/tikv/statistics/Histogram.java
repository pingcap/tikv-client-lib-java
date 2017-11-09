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
 *
 */

package com.pingcap.tikv.statistics;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.exception.HistogramException;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.meta.TiKey;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.Types;
import com.pingcap.tikv.util.Bucket;
import com.pingcap.tikv.util.DBReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Histogram {

  private static final String TABLE_ID = "table_id"; //the ID of table
  private static final String IS_INDEX = "is_index"; // whether or not have an index
  private static final String HIST_ID = "hist_id"; //ColumnWithHistogram ID for each histogram
  private static final String BUCKET_ID = "bucket_id"; //the ID of bucket
  private static final String COUNT = "count"; //the total number of bucket
  private static final String REPEATS = "repeats"; //repeats values in histogram
  private static final String LOWER_BOUND = "lower_bound"; //lower bound of histogram
  private static final String UPPER_BOUND = "upper_bound"; //upper bound of histogram

  //Histogram
  private long id;
  private long numberOfDistinctValue; // Number of distinct values.
  private List<Bucket> buckets;
  private long nullCount;
  private long lastUpdateVersion;


  public Histogram() {
  }

  public Histogram(long id,
                   long numberOfDistinctValue,
                   List<Bucket> buckets) {
    this.id = id;
    this.numberOfDistinctValue = numberOfDistinctValue;
    this.buckets = buckets;
    this.nullCount = 0;
    this.lastUpdateVersion = 0;
  }

  public Histogram(long id,
                   long numberOfDistinctValue,
                   List<Bucket> buckets,
                   long nullCount,
                   long lastUpdateVersion) {
    this.id = id;
    this.numberOfDistinctValue = numberOfDistinctValue;
    this.buckets = buckets;
    this.nullCount = nullCount;
    this.lastUpdateVersion = lastUpdateVersion;
  }

  public long getNumberOfDistinctValue() {
    return numberOfDistinctValue;
  }

  void setNumberOfDistinctValue(long numberOfDistinctValue) {
    this.numberOfDistinctValue = numberOfDistinctValue;
  }

  public List<Bucket> getBuckets() {
    return buckets;
  }

  public void setBuckets(List<Bucket> buckets) {
    this.buckets = buckets;
  }

  public long getNullCount() {
    return nullCount;
  }

  void setNullCount(long nullCount) {
    this.nullCount = nullCount;
  }

  long getLastUpdateVersion() {
    return lastUpdateVersion;
  }

  void setLastUpdateVersion(long lastUpdateVersion) {
    this.lastUpdateVersion = lastUpdateVersion;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  /** Loads histogram from storage */
  Histogram histogramFromStorage(
      long tableID, long colID, long isIndex, long distinct, long lastUpdateVersion,
      long nullCount, DBReader dbReader, DataType type) {

    TiTableInfo table = dbReader.getTableInfo("stats_buckets");
    List<TiExpr> firstAnd =
        ImmutableList.of(
            new Equal(TiColumnRef.create(TABLE_ID, table), TiConstant.create(tableID)),
            new Equal(TiColumnRef.create(IS_INDEX, table), TiConstant.create(isIndex)),
            new Equal(TiColumnRef.create(HIST_ID, table), TiConstant.create(colID))
        );

    List<String> returnFields = ImmutableList.of(BUCKET_ID, COUNT, REPEATS, LOWER_BOUND, UPPER_BOUND, TABLE_ID, IS_INDEX, HIST_ID);
    List<Row> rows = dbReader.getSelectedRows("stats_buckets", firstAnd, returnFields);

    this.id = colID;
    this.numberOfDistinctValue = distinct;
    this.lastUpdateVersion = lastUpdateVersion;
    this.nullCount = nullCount;

    int len = 0;
    List<Bucket> tmpBuckets = new ArrayList<>(rows.size());
    for(int i = 0; i < rows.size(); i ++) tmpBuckets.add(new Bucket(TiKey.create(1)));

    for (Row row: rows) {
      long bucketID = row.getLong(0);
      long count = row.getLong(1);
      long repeats = row.getLong(2);
      TiKey lowerBound, upperBound;
      try {
        if (isIndex == 1) {
          lowerBound = TiKey.encode(row.get(3, DataTypeFactory.of(Types.TYPE_BLOB)));
          upperBound = TiKey.encode(row.get(4, DataTypeFactory.of(Types.TYPE_BLOB)));
        } else {
          lowerBound = TiKey.encode(row.get(3, type));
          upperBound = TiKey.encode(row.get(4, type));
        }
        tmpBuckets.add((int) bucketID, new Bucket(count, repeats, lowerBound, upperBound));
      } catch (HistogramException e) {
        throw new HistogramException("Bucket size too large");
      }
      ++ len;
    }
    buckets = tmpBuckets.subList(0, len);
    for (int i = 1; i < buckets.size(); i++) {
      buckets.get(i).count += buckets.get(i - 1).count;
    }
    return this;
  }

  /** equalRowCount estimates the row count where the column equals to value. */
  double equalRowCount(TiKey values) {
    int index = lowerBound(values);
    //index not in range
    if (index == -buckets.size() - 1) {
      return 0;
    }
    // index found
    if (index >= 0) {
      return buckets.get(index).repeats;
    }
    //index not found
    index = -index - 1;
    int cmp;
    if (buckets.get(index).lowerBound == null) {
      cmp = 1;
    } else {
      Objects.requireNonNull(buckets.get(index).lowerBound);
      cmp = values.compareTo(buckets.get(index).lowerBound);
    }
    if (cmp < 0) {
      return 0;
    }
    return totalRowCount() / numberOfDistinctValue;
  }

  /** greaterRowCount estimates the row count where the column greater than value. */
  double greaterRowCount(TiKey values) {
    double lessCount = lessRowCount(values);
    double equalCount = equalRowCount(values);
    double greaterCount;
    greaterCount = totalRowCount() - lessCount - equalCount;
    if (greaterCount < 0) {
      greaterCount = 0;
    }
    return greaterCount;
  }

  /** greaterAndEqRowCount estimates the row count where the column less than or equal to value. */
  private double greaterAndEqRowCount(TiKey values) {
    double greaterCount = greaterRowCount(values);
    double equalCount = equalRowCount(values);
    return greaterCount + equalCount;
  }

  /** lessRowCount estimates the row count where the column less than value. */
  double lessRowCount(TiKey values) {
    int index = lowerBound(values);
    //index not in range
    if (index == -buckets.size() - 1) {
      return totalRowCount();
    }
    if (index < 0) {
      index = -index - 1;
    }
    double curCount = buckets.get(index).count;
    double preCount = 0;
    if (index > 0) {
      preCount = buckets.get(index - 1).count;
    }
    double lessThanBucketValueCount = curCount - buckets.get(index).repeats;
    TiKey lowerBound = buckets.get(index).lowerBound;
    int c;
    if(lowerBound != null) {
      c = values.compareTo(lowerBound);
    } else {
      c = 1;
    }
    if (c <= 0) {
      return preCount;
    }
    return (preCount + lessThanBucketValueCount) / 2;
  }

  /** lessAndEqRowCount estimates the row count where the column less than or equal to value. */
  public double lessAndEqRowCount(TiKey values) {
    double lessCount = lessRowCount(values);
    double equalCount = equalRowCount(values);
    return lessCount + equalCount;
  }

  /** betweenRowCount estimates the row count where column greater than or equal to a and less than b. */
  double betweenRowCount(TiKey a, TiKey b) {
//    CodecDataInput c = new CodecDataInput(a.getByteString());
//    CodecDataInput d = new CodecDataInput(b.getByteString());
//    System.out.println(c.readLine() + " with " + d.readLine());
    double lessCountA = lessRowCount(a);
    double lessCountB = lessRowCount(b);
    if (lessCountA >= lessCountB) {
      return inBucketBetweenCount();
    }
    return lessCountB - lessCountA;
  }

  protected double totalRowCount() {
    if (buckets.isEmpty()) {
      return 0;
    }
    return (buckets.get(buckets.size() - 1).count);
  }

  protected double bucketRowCount() {
    return totalRowCount() / buckets.size();
  }

  protected double inBucketBetweenCount() {
    // TODO: Make this estimation more accurate using uniform spread assumption.
    return bucketRowCount() / 3 + 1;
  }

  /**
   * lowerBound returns the smallest index of the searched key
   * and returns (-[insertion point] - 1) if the key is not found in buckets
   * where [insertion point] denotes the index of the first element greater than the key
   */
  protected int lowerBound(TiKey key) {
    assert key.getClass() == buckets.get(0).getUpperBound().getClass();
//    System.out.println(">>>>>>>>>>>>" + TiKey.unwrap(key).getClass() + " " + TiKey.unwrap(buckets.get(0).getUpperBound()).getClass());
//    for(Bucket bucket: buckets) {
//      System.out.println(bucket);
//    }
//    System.out.println("<<<<<<<<<<<<");
    return Arrays.binarySearch(buckets.toArray(), new Bucket(key));
  }

  /**
   * mergeBuckets is used to merge every two neighbor buckets.
   * @param  bucketIdx: index of the last bucket.
   */
  void mergeBlock(int bucketIdx) {
    int curBuck = 0;
    for (int i = 0; i + 1 <= bucketIdx; i += 2) {
      buckets.set(curBuck++, new Bucket(buckets.get(i + 1).count,
          buckets.get(i + 1).repeats,
          buckets.get(i + 1).lowerBound,
          buckets.get(i).upperBound));
    }
    if (bucketIdx % 2 == 0) {
      buckets.set(curBuck++, buckets.get(bucketIdx));
    }
    buckets = buckets.subList(0, curBuck);
  }

  /** getIncreaseFactor will return a factor of data increasing after the last analysis. */
  double getIncreaseFactor(long totalCount) {
    long columnCount = buckets.get(buckets.size() - 1).count + nullCount;
    if (columnCount == 0) {
      return 1.0;
    }
    return (double) totalCount / (double) columnCount;
  }
}
