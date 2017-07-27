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

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.types.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MetaUtils {
  public static class TableBuilder {
    static long autoId = 1;

    private static long newId() {
      return autoId++;
    }

    public static TableBuilder newBuilder() {
      return new TableBuilder();
    }

    private boolean pkHandle;
    private String name;
    private List<TiColumnInfo> columns = new ArrayList<>();
    private List<TiIndexInfo> indices = new ArrayList<>();

    public TableBuilder() {}

    public TableBuilder name(String name) {
      this.name = name;
      return this;
    }

    public TableBuilder addColumn(String name, DataType type) {
      return addColumn(name, type, false);
    }

    public TableBuilder addColumn(String name, DataType type, boolean pk) {
      for (TiColumnInfo c : columns) {
        if (c.matchName(name)) {
          throw new TiClientInternalException("duplicated name: " + name);
        }
      }

      TiColumnInfo col = new TiColumnInfo(newId(), name, columns.size(), type, pk);
      columns.add(col);
      return this;
    }

    public TableBuilder appendIndex(String indexName, List<String> colNames, boolean isPk) {
      List<TiIndexColumn> indexCols =
          colNames
              .stream()
              .map(name -> columns.stream().filter(c -> c.matchName(name)).findFirst())
              .flatMap(col -> col.isPresent() ? Stream.of(col.get()) : Stream.empty())
              .map(TiColumnInfo::toIndexColumn)
              .collect(Collectors.toList());

      TiIndexInfo index =
          new TiIndexInfo(
              newId(),
              CIStr.newCIStr(indexName),
              CIStr.newCIStr(name),
              ImmutableList.copyOf(indexCols),
              false,
              isPk,
              SchemaState.StatePublic.getStateCode(),
              "",
              IndexType.IndexTypeBtree.getTypeCode(),
              false);
      indices.add(index);
      return this;
    }

    public TableBuilder setPkHandle(boolean pkHandle) {
      this.pkHandle = pkHandle;
      return this;
    }

    public TiTableInfo build() {
      long tid = newId();
      if (name == null) {
        name = "Table" + tid;
      }
      return new TiTableInfo(
          tid, CIStr.newCIStr(name), "", "", pkHandle, columns, indices, "", 0, 0, 0, 0);
    }
  }
}
