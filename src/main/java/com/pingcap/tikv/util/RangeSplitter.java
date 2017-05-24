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


import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.RegionManager;
import com.pingcap.tikv.grpc.Metapb;
import com.pingcap.tikv.meta.TiRange;

import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class RangeSplitter {
    public static RangeSplitter newSplitter(RegionManager mgr) {
        return new RangeSplitter(mgr);
    }

    private RangeSplitter(RegionManager regionManager) {
        this.regionManager = regionManager;
    }

    protected final RegionManager regionManager;

    public List<Pair<Pair<Metapb.Region, Metapb.Store>, TiRange<ByteString>>>
    splitRangeByRegion(List<TiRange<ByteString>> keyRanges) {
        checkArgument(keyRanges != null && keyRanges.size() != 0);
        int i = 0;
        TiRange<ByteString> range = keyRanges.get(i++);
        Comparator<ByteString> comp = range.getComparator();
        ImmutableList.Builder<Pair<Pair<Metapb.Region, Metapb.Store>, TiRange<ByteString>>> resultBuilder = ImmutableList.builder();
        while (true) {
            Pair<Metapb.Region, Metapb.Store> pair = regionManager.getRegionStorePairByKey(range.getLowValue());
            Metapb.Region region = pair.first;
            ByteString startKey = range.getLowValue();

            // TODO: Deal with open close range more carefully
            if (region.getEndKey().size() != 0 &&
                    comp.compare(range.getHighValue(), region.getEndKey()) >= 0) {
                // Current Range not ended
                TiRange<ByteString> mappedRange =
                        TiRange.createByteStringRange(startKey, region.getEndKey(), range.isLeftOpen(), true);
                resultBuilder.add(Pair.create(pair, mappedRange));
                range = TiRange.createByteStringRange(region.getEndKey(), range.getHighValue(), false, range.isRightOpen());
            } else {
                TiRange<ByteString> mappedRange =
                        TiRange.createByteStringRange(startKey, range.getHighValue(), range.isLeftOpen(), range.isRightOpen());
                resultBuilder.add(Pair.create(pair, mappedRange));
                if (i >= keyRanges.size()) {
                    break;
                }
                range = keyRanges.get(i++);
            }
        }
        return resultBuilder.build();
    }
}
