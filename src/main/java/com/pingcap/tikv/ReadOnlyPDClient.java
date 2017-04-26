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

package com.pingcap.tikv;


import com.google.protobuf.ByteString;
import com.pingcap.tikv.grpc.Metapb.Region;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.meta.TiTimestamp;

import java.util.concurrent.Future;

/**
 * Readonly PD client including only reading related interface
 * Supposed for TiDB-like use cases
 */
public interface ReadOnlyPDClient {
    /**
     * <p>Get Timestamp from Placement Driver</p>
     *
     * @return a timestamp object
     */
    TiTimestamp getTimestamp();

    /**
     * <p>Get Region from PD by key specified</p>
     *
     * @param key key in bytes for locating a region
     * @return the region whose startKey and endKey range covers the given key
     */
    Region getRegionByKey(ByteString key);
    Future<Region> getRegionByKeyAsync(ByteString key);

    /**
     * <p>Get Region by Region Id</p>
     *
     * @param id Region Id
     * @return the region corresponding to the given Id
     */
    Region getRegionByID(long id);
    Future<Region> getRegionByIDAsync(long id);

    /**
     * <p>Get Store by StoreId</p>
     *
     * @param storeId StoreId
     * @return the Store corresponding to the given Id
     */
    Store getStore(long storeId);
    Future<Store> getStoreAsync(long storeId);

    /**
     * <p>Close underlining resources</p>
     *
     * @throws InterruptedException
     */
    void close() throws InterruptedException;

    /**
     * <p>Get associated session</p>
     *
     * * @return the session associated to client
     */
    TiSession getSession();
}
