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

import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.RowMeta;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.operation.ChunkIterator;
import com.pingcap.tikv.row.ObjectRowImpl;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.util.Bucket;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.pingcap.tikv.types.Types.TYPE_BLOB;
import static com.pingcap.tikv.types.Types.TYPE_LONG;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Created by birdstorm on 2017/8/13.
 *
 */
public class HistogramTest {
  private static List<Chunk> chunks = new ArrayList<>();
  private static Histogram histogram = new Histogram();

  @Before
  public void histogramValidationTests() throws Exception {
    /*
     * +----------+----------+---------+-----------+-------+---------+-------------+-------------+
     * | table_id | is_index | hist_id | bucket_id | count | repeats | upper_bound | lower_bound |
     * +----------+----------+---------+-----------+-------+---------+-------------+-------------+
     * |       27 |        0 |       1 |         0 |     1 |       1 | 1           | 1           |
     * |       27 |        0 |       1 |         1 |     1 |       1 | 5           | 3           |
     * |       27 |        1 |       2 |         0 |     1 |       1 | 1           | 1           |
     * +----------+----------+---------+-----------+-------+---------+-------------+-------------+
     */
    String histogramStr = "\b6\b\000\b\002\b\000\b\002\b\002\002\0021\002\0021\b6\b\000\b\002\b\002\b\002\b\002\002\0025\002\0023\b6" +
        "\b\002\b\004\b\000\b\002\b\002\002\0021\002\0021";
    Chunk chunk =
        Chunk.newBuilder()
            .setRowsData(ByteString.copyFromUtf8(histogramStr))
            .addRowsMeta(0, RowMeta.newBuilder().setHandle(6).setLength(18))
            .addRowsMeta(1, RowMeta.newBuilder().setHandle(7).setLength(18))
            .addRowsMeta(2, RowMeta.newBuilder().setHandle(8).setLength(18))
            .build();

    chunks.add(chunk);
    ChunkIterator chunkIterator = new ChunkIterator(chunks);
    com.pingcap.tikv.types.DataType blobs = DataTypeFactory.of(TYPE_BLOB);
    com.pingcap.tikv.types.DataType ints = DataTypeFactory.of(TYPE_LONG);
    Row row = ObjectRowImpl.create(24);
    CodecDataInput cdi = new CodecDataInput(chunkIterator.next());
    ints.decodeValueToRow(cdi, row, 0);
    ints.decodeValueToRow(cdi, row, 1);
    ints.decodeValueToRow(cdi, row, 2);
    ints.decodeValueToRow(cdi, row, 3);
    ints.decodeValueToRow(cdi, row, 4);
    ints.decodeValueToRow(cdi, row, 5);
    blobs.decodeValueToRow(cdi, row, 6);
    blobs.decodeValueToRow(cdi, row, 7);
    cdi = new CodecDataInput(chunkIterator.next());
    ints.decodeValueToRow(cdi, row, 8);
    ints.decodeValueToRow(cdi, row, 9);
    ints.decodeValueToRow(cdi, row, 10);
    ints.decodeValueToRow(cdi, row, 11);
    ints.decodeValueToRow(cdi, row, 12);
    ints.decodeValueToRow(cdi, row, 13);
    blobs.decodeValueToRow(cdi, row, 14);
    blobs.decodeValueToRow(cdi, row, 15);
    cdi = new CodecDataInput(chunkIterator.next());
    ints.decodeValueToRow(cdi, row, 16);
    ints.decodeValueToRow(cdi, row, 17);
    ints.decodeValueToRow(cdi, row, 18);
    ints.decodeValueToRow(cdi, row, 19);
    ints.decodeValueToRow(cdi, row, 20);
    ints.decodeValueToRow(cdi, row, 21);
    blobs.decodeValueToRow(cdi, row, 22);
    blobs.decodeValueToRow(cdi, row, 23);


    assertEquals(row.getLong(0), 27);
    assertEquals(row.getLong(1), 0);
    assertEquals(row.getLong(2), 1);
    assertEquals(row.getLong(3), 0);
    assertEquals(row.getLong(4), 1);
    assertEquals(row.getLong(5), 1);
    assertArrayEquals(row.getBytes(6), ByteString.copyFromUtf8("1").toByteArray());
    assertArrayEquals(row.getBytes(7), ByteString.copyFromUtf8("1").toByteArray());
    assertEquals(row.getLong(8), 27);
    assertEquals(row.getLong(9), 0);
    assertEquals(row.getLong(10), 1);
    assertEquals(row.getLong(11), 1);
    assertEquals(row.getLong(12), 1);
    assertEquals(row.getLong(13), 1);
    assertArrayEquals(row.getBytes(14), ByteString.copyFromUtf8("5").toByteArray());
    assertArrayEquals(row.getBytes(15), ByteString.copyFromUtf8("3").toByteArray());
    assertEquals(row.getLong(16), 27);
    assertEquals(row.getLong(17), 1);
    assertEquals(row.getLong(18), 2);
    assertEquals(row.getLong(19), 0);
    assertEquals(row.getLong(20), 1);
    assertEquals(row.getLong(21), 1);
    assertArrayEquals(row.getBytes(22), ByteString.copyFromUtf8("1").toByteArray());
    assertArrayEquals(row.getBytes(23), ByteString.copyFromUtf8("1").toByteArray());
  }

  @Before
  //a temperate function to allow unit tests
  public void createSampleHistogram() throws Exception {

    /*
     * +---------+-----------+-------+---------+-------------+-------------+
     * | hist_id | bucket_id | count | repeats | upper_bound | lower_bound |
     * +---------+-----------+-------+---------+-------------+-------------+
     * |      10 |         0 |     5 |       5 | 3           | 3           |
     * |      10 |         1 |    10 |       5 | 8           | 8           |
     * |      10 |         2 |    25 |      15 | 100         | 100         |
     * |      10 |         3 |    30 |       1 | 200         | 101         |
     * +---------+-----------+-------+---------+-------------+-------------+
     */

    histogram.setId(10);
    histogram.setNullCount(4);
    histogram.setLastUpdateVersion(23333333);
    histogram.setNumberOfDistinctValue(5);
    histogram.setNullCount(15);
    Bucket[] buckets = new Bucket[4];
    for(int i = 0; i < 4; i ++) {
      buckets[i] = new Bucket();
    }
    buckets[0].setCount(5);
    buckets[0].setRepeats(5);
    buckets[0].setLowerBound(3);
    buckets[0].setUpperBound(3);
    buckets[1].setCount(10);
    buckets[1].setRepeats(5);
    buckets[1].setLowerBound(8);
    buckets[1].setUpperBound(8);
    buckets[2].setCount(25);
    buckets[2].setRepeats(15);
    buckets[2].setLowerBound(100);
    buckets[2].setUpperBound(100);
    buckets[3].setCount(30);
    buckets[3].setRepeats(1);
    buckets[3].setLowerBound(101);
    buckets[3].setUpperBound(200);
    histogram.setBuckets(buckets);
  }

  @Test
  public void testEqualRowCount() throws Exception {
    assertEquals(histogram.equalRowCount(8), 5.0, 0.000001);
    assertEquals(histogram.equalRowCount(150), 6.0, 0.000001);
  }

  @Test
  public void testGreaterRowCount() throws Exception {
    assertEquals(histogram.greaterRowCount(0), 30.0, 0.000001);
    assertEquals(histogram.greaterRowCount(3), 25.0, 0.000001);
    assertEquals(histogram.greaterRowCount(4), 25.0, 0.000001);
    assertEquals(histogram.greaterRowCount(9), 20.0, 0.000001);
    assertEquals(histogram.greaterRowCount(100), 5.0, 0.000001);
    assertEquals(histogram.greaterRowCount(150), 0.0, 0.000001);
    assertEquals(histogram.greaterRowCount(1000), 0.0, 0.000001);
  }

  @Test
  public void testBetweenRowCount() throws Exception {
    assertEquals(histogram.betweenRowCount(12, 150), 17.0, 0.000001);
    assertEquals(histogram.betweenRowCount(102, 160), 3.5, 0.000001);
  }

  @Test
  public void testTotalRowCount() throws Exception {
    assertEquals(histogram.totalRowCount(), 30.0, 0.000001);
  }

  @Test
  public void testLessRowCount() throws Exception {
    assertEquals(histogram.lessRowCount(0), 0.0, 0.000001);
    assertEquals(histogram.lessRowCount(3), 0.0, 0.000001);
    assertEquals(histogram.lessRowCount(4), 5.0, 0.000001);
    assertEquals(histogram.lessRowCount(9), 10.0, 0.000001);
    assertEquals(histogram.lessRowCount(100), 10.0, 0.000001);
    assertEquals(histogram.lessRowCount(150), 27.0, 0.000001);
    assertEquals(histogram.lessRowCount(1000), 30.0, 0.000001);
  }

  @Test
  public void testLowerBound() throws Exception {
    assertEquals(histogram.lowerBound(0), -1);
    assertEquals(histogram.lowerBound(3), 0);
    assertEquals(histogram.lowerBound(4), -2);
    assertEquals(histogram.lowerBound(8), 1);
    assertEquals(histogram.lowerBound(99), -3);
    assertEquals(histogram.lowerBound(100), 2);
    assertEquals(histogram.lowerBound(101), -4);
    assertEquals(histogram.lowerBound(1000), -5);
  }

  @Test
  public void testMergeBlock() throws Exception {
    histogram.mergeBlock(histogram.getBuckets().length - 1);
    assertEquals(histogram.getBuckets().length, 2);
  }

}