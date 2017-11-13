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
import com.pingcap.tikv.meta.TiKey;
import com.pingcap.tikv.operation.ChunkIterator;
import com.pingcap.tikv.row.ObjectRowImpl;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;
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
     * |       27 |        0 |       1 |         0 |     2 |       1 | 1           | 0           |
     * |       27 |        0 |       1 |         1 |     3 |       1 | 3           | 2           |
     * +----------+----------+---------+-----------+-------+---------+-------------+-------------+
     */
    String histogramStr = "\b6\b\000\b\002\b\000\b\004\b\002\002\0021\002\0020\b6\b\000\b\002\b\002\b\006\b\002\002\0023\002\0022";
    Chunk chunk =
        Chunk.newBuilder()
            .setRowsData(ByteString.copyFromUtf8(histogramStr))
            .addRowsMeta(0, RowMeta.newBuilder().setHandle(6).setLength(18))
            .addRowsMeta(1, RowMeta.newBuilder().setHandle(7).setLength(18))
            .build();

    chunks.add(chunk);
    ChunkIterator<ByteString> chunkIterator = ChunkIterator.getRawBytesChunkIterator(chunks);
    DataType blobs = DataTypeFactory.of(TYPE_BLOB);
    DataType ints = DataTypeFactory.of(TYPE_LONG);
    List<Row> rows = new ArrayList<>();
    Row row = ObjectRowImpl.create(8);
    CodecDataInput cdi = new CodecDataInput(chunkIterator.next());
    ints.decodeValueToRow(cdi, row, 0);
    ints.decodeValueToRow(cdi, row, 1);
    ints.decodeValueToRow(cdi, row, 2);
    ints.decodeValueToRow(cdi, row, 3);
    ints.decodeValueToRow(cdi, row, 4);
    ints.decodeValueToRow(cdi, row, 5);
    blobs.decodeValueToRow(cdi, row, 6);
    blobs.decodeValueToRow(cdi, row, 7);
    rows.add(row);

    row = ObjectRowImpl.create(8);
    cdi = new CodecDataInput(chunkIterator.next());
    ints.decodeValueToRow(cdi, row, 0);
    ints.decodeValueToRow(cdi, row, 1);
    ints.decodeValueToRow(cdi, row, 2);
    ints.decodeValueToRow(cdi, row, 3);
    ints.decodeValueToRow(cdi, row, 4);
    ints.decodeValueToRow(cdi, row, 5);
    blobs.decodeValueToRow(cdi, row, 6);
    blobs.decodeValueToRow(cdi, row, 7);
    rows.add(row);

    assertEquals(rows.get(0).getLong(0), 27);
    assertEquals(rows.get(0).getLong(1), 0);
    assertEquals(rows.get(0).getLong(2), 1);
    assertEquals(rows.get(0).getLong(3), 0);
    assertEquals(rows.get(0).getLong(4), 2);
    assertEquals(rows.get(0).getLong(5), 1);
    assertArrayEquals(rows.get(0).getBytes(6), ByteString.copyFromUtf8("1").toByteArray());
    assertArrayEquals(rows.get(0).getBytes(7), ByteString.copyFromUtf8("0").toByteArray());
    assertEquals(rows.get(1).getLong(0), 27);
    assertEquals(rows.get(1).getLong(1), 0);
    assertEquals(rows.get(1).getLong(2), 1);
    assertEquals(rows.get(1).getLong(3), 1);
    assertEquals(rows.get(1).getLong(4), 3);
    assertEquals(rows.get(1).getLong(5), 1);
    assertArrayEquals(rows.get(1).getBytes(6), ByteString.copyFromUtf8("3").toByteArray());
    assertArrayEquals(rows.get(1).getBytes(7), ByteString.copyFromUtf8("2").toByteArray());
  }

  @Before
  //a temperate function to allow unit tests
  public void createSampleHistogram() throws Exception {

    /*
     * +---------+-----------+-------+---------+-------------+-------------+
     * | hist_id | bucket_id | count | repeats | upper_bound | lower_bound |
     * +---------+-----------+-------+---------+-------------+-------------+
     * |      10 |         0 |     5 |       2 | 3           | 0           |
     * |      10 |         1 |     5 |       1 | 7           | 4           |
     * |      10 |         2 |    15 |       4 | 11          | 8           |
     * |      10 |         3 |     5 |       0 | 15          | 12          |
     * +---------+-----------+-------+---------+-------------+-------------+
     */

    histogram.setId(10);
    histogram.setNullCount(4);
    histogram.setLastUpdateVersion(23333333);
    histogram.setNumberOfDistinctValue(10);
    ArrayList<Bucket> buckets = new ArrayList<>(4);
    buckets.add(new Bucket(5, 2, 0, 3));
    buckets.add(new Bucket(10, 1, 4, 7));
    buckets.add(new Bucket(25, 4, 8, 11));
    buckets.add(new Bucket(30, 0, 12, 15));
    histogram.setBuckets(buckets);
  }

  @Test
  public void testEqualRowCount() throws Exception {
    assertEquals(histogram.equalRowCount(TiKey.encode(4)), 3.0, 0.000001);
    assertEquals(histogram.equalRowCount(TiKey.encode(11)), 4.0, 0.000001);
  }

  @Test
  public void testGreaterRowCount() throws Exception {
    assertEquals(histogram.greaterRowCount(TiKey.encode(-1)), 30.0, 0.000001);
    assertEquals(histogram.greaterRowCount(TiKey.encode(0)), 27.0, 0.000001);
    assertEquals(histogram.greaterRowCount(TiKey.encode(4)), 22.0, 0.000001);
    assertEquals(histogram.greaterRowCount(TiKey.encode(9)), 11.5, 0.000001);
    assertEquals(histogram.greaterRowCount(TiKey.encode(11)), 10.5, 0.000001); //shouldn't this be 5.0?
    assertEquals(histogram.greaterRowCount(TiKey.encode(12)), 2.0, 0.000001);
    assertEquals(histogram.greaterRowCount(TiKey.encode(19)), 0.0, 0.000001);
  }

  @Test
  public void testBetweenRowCount() throws Exception {
    assertEquals(histogram.betweenRowCount(TiKey.encode(2), TiKey.encode(6)), 5.5, 0.000001);
    assertEquals(histogram.betweenRowCount(TiKey.encode(8), TiKey.encode(10)), 5.5, 0.000001);
  }

  @Test
  public void testTotalRowCount() throws Exception {
    assertEquals(histogram.totalRowCount(), 30.0, 0.000001);
  }

  @Test
  public void testLessRowCount() throws Exception {
    assertEquals(histogram.lessRowCount(TiKey.encode(0)), 0.0, 0.000001);
    assertEquals(histogram.lessRowCount(TiKey.encode(3)), 1.5, 0.000001);
    assertEquals(histogram.lessRowCount(TiKey.encode(4)), 5.0, 0.000001);
    assertEquals(histogram.lessRowCount(TiKey.encode(7)), 7.0, 0.000001);
    assertEquals(histogram.lessRowCount(TiKey.encode(9)), 15.5, 0.000001);
    assertEquals(histogram.lessRowCount(TiKey.encode(12)), 25.0, 0.000001);
    assertEquals(histogram.lessRowCount(TiKey.encode(15)), 27.5, 0.000001); //shouldn't this be 30.0?
  }

  @Test
  public void testLowerBound() throws Exception {
    assertEquals(histogram.lowerBound(TiKey.encode(0)), -1);
    assertEquals(histogram.lowerBound(TiKey.encode(3)), 0);
    assertEquals(histogram.lowerBound(TiKey.encode(4)), -2);
    assertEquals(histogram.lowerBound(TiKey.encode(7)), 1);
    assertEquals(histogram.lowerBound(TiKey.encode(9)), -3);
    assertEquals(histogram.lowerBound(TiKey.encode(11)), 2);
    assertEquals(histogram.lowerBound(TiKey.encode(13)), -4);
    assertEquals(histogram.lowerBound(TiKey.encode(19)), -5);
  }

  @Test
  public void testMergeBlock() throws Exception {
    histogram.mergeBlock(histogram.getBuckets().size() - 1);
    assertEquals(histogram.getBuckets().size(), 2);
  }

}