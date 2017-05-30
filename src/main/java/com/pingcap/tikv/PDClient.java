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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.grpc.Metapb;
import com.pingcap.tikv.grpc.Metapb.Region;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.grpc.PDGrpc;
import com.pingcap.tikv.grpc.PDGrpc.PDBlockingStub;
import com.pingcap.tikv.grpc.PDGrpc.PDStub;
import com.pingcap.tikv.grpc.Pdpb.*;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.util.FutureObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class PDClient extends AbstractGrpcClient<PDBlockingStub, PDStub> implements ReadOnlyPDClient {
    private RequestHeader               header;
    private TsoRequest                  tsoReq;
    private volatile LeaderWrapper      leaderWrapper;
    private ScheduledExecutorService    service;

    @Override
    public TiTimestamp getTimestamp() {
        FutureObserver<Timestamp, TsoResponse> responseObserver =
                new FutureObserver<>((TsoResponse resp) -> resp.getTimestamp());
        StreamObserver<TsoRequest> requestObserver = callBidiStreamingWithRetry(PDGrpc.METHOD_TSO, responseObserver);

        requestObserver.onNext(tsoReq);
        requestObserver.onCompleted();
        try {
            Timestamp resp = responseObserver.getFuture().get();
            return new TiTimestamp(resp.getPhysical(), resp.getLogical());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new GrpcException(e);
        }
        return null;
    }

    @Override
    public Region getRegionByKey(ByteString key) {
        GetRegionRequest request = GetRegionRequest.newBuilder()
                .setHeader(header)
                .setRegionKey(key)
                .build();

        GetRegionResponse resp = callWithRetry(PDGrpc.METHOD_GET_REGION, request);
        return resp.getRegion();
    }

    @Override
    public Future<Region> getRegionByKeyAsync(ByteString key) {
        FutureObserver<Region, GetRegionResponse> responseObserver =
                new FutureObserver<>((GetRegionResponse resp) -> resp.getRegion());
        GetRegionRequest request = GetRegionRequest.newBuilder()
                .setHeader(header)
                .setRegionKey(key)
                .build();

        callAsyncWithRetry(PDGrpc.METHOD_GET_REGION, request, responseObserver);
        return responseObserver.getFuture();
    }

    @Override
    public Region getRegionByID(long id) {
        GetRegionByIDRequest request = GetRegionByIDRequest.newBuilder()
                .setHeader(header)
                .setRegionId(id)
                .build();

        GetRegionResponse resp = callWithRetry(PDGrpc.METHOD_GET_REGION_BY_ID, request);
        // Instead of using default leader instance, explicitly set no leader to null
        return resp.getRegion();
    }

    @Override
    public Future<Region> getRegionByIDAsync(long id) {
        FutureObserver<Region, GetRegionResponse> responseObserver =
                new FutureObserver<>((GetRegionResponse resp) -> resp.getRegion());

        GetRegionByIDRequest request = GetRegionByIDRequest.newBuilder()
                .setHeader(header)
                .setRegionId(id)
                .build();

        callAsyncWithRetry(PDGrpc.METHOD_GET_REGION_BY_ID, request, responseObserver);
        return responseObserver.getFuture();
    }

    @Override
    public Store getStore(long storeId) {
        GetStoreRequest request = GetStoreRequest.newBuilder()
                .setHeader(header)
                .setStoreId(storeId)
                .build();

        GetStoreResponse resp = callWithRetry(PDGrpc.METHOD_GET_STORE, request);
        Store store = resp.getStore();
        if (store.getState() == Metapb.StoreState.Tombstone) {
            return null;
        }
        return store;
    }

    @Override
    public Future<Store> getStoreAsync(long storeId) {
        FutureObserver<Store, GetStoreResponse> responseObserver =
                new FutureObserver<>((GetStoreResponse resp) -> {
                    Store store = resp.getStore();
                    if (store.getState() == Metapb.StoreState.Tombstone) {
                        return null;
                    }
                    return store;
        });

        GetStoreRequest request = GetStoreRequest.newBuilder()
                .setHeader(header)
                .setStoreId(storeId)
                .build();

        callAsyncWithRetry(PDGrpc.METHOD_GET_STORE, request, responseObserver);
        return responseObserver.getFuture();
    }

    @Override
    public void close() throws InterruptedException {
        if (service != null) {
            service.shutdownNow();
        }
        if (getLeaderWrapper() != null) {
            getLeaderWrapper().close();
        }
    }

    public static ReadOnlyPDClient create(TiSession session) {
        return createRaw(session);
    }

    @Override
    protected Callable<Void> getRecoveryMethod() {
        return () -> { updateLeader(getMembers()); return null; };
    }

    @VisibleForTesting
    RequestHeader getHeader() {
        return header;
    }

    @VisibleForTesting
    LeaderWrapper getLeaderWrapper() {
        return leaderWrapper;
    }

    class LeaderWrapper {
        private final HostAndPort           leaderInfo;
        private final PDBlockingStub        blockingStub;
        private final PDStub                asyncStub;
        private final ManagedChannel        channel;
        private final long                  createTime;

        public LeaderWrapper(HostAndPort leaderInfo,
                             PDGrpc.PDBlockingStub blockingStub,
                             PDGrpc.PDStub asyncStub,
                             ManagedChannel channel,
                             long createTime) {
            this.leaderInfo = leaderInfo;
            this.blockingStub = blockingStub;
            this.asyncStub = asyncStub;
            this.channel = channel;
            this.createTime = createTime;
        }

        public HostAndPort getLeaderInfo() {
            return leaderInfo;
        }

        public PDBlockingStub getBlockingStub() { return blockingStub; }

        public PDStub getAsyncStub() {
            return asyncStub;
        }

        public long getCreateTime() {
            return createTime;
        }

        public void close() {
            channel.isShutdown();
        }
    }

    private ManagedChannel getManagedChannel(HostAndPort url) {
        return ManagedChannelBuilder
                .forAddress(url.getHostText(), url.getPort())
                .usePlaintext(true)
                .build();
    }

    private GetMembersResponse getMembers() {
        List<HostAndPort> pdAddrs = getConf().getPdAddrs();
        checkArgument(pdAddrs.size() > 0, "No PD address specified.");
        for (HostAndPort url : pdAddrs) {
            ManagedChannel probChan = null;
            try {
                probChan = getManagedChannel(url);
                PDGrpc.PDBlockingStub stub = PDGrpc.newBlockingStub(probChan);
                GetMembersRequest request = GetMembersRequest.newBuilder()
                        .setHeader(RequestHeader.getDefaultInstance())
                        .build();
                return stub.getMembers(request);
            }
            catch (Exception ignore) {}
            finally {
                if (probChan != null) {
                    probChan.shutdownNow();
                }
            }
        }
        return null;
    }

    private void updateLeader(GetMembersResponse resp) {
        String leaderUrlStr = "URL Not Set";
        try {
            long ts = System.nanoTime();
            synchronized (this) {
                // Lock for not flooding during pd error
                if (leaderWrapper != null && leaderWrapper.getCreateTime() > ts) return;

                if (resp == null) {
                    resp = getMembers();
                    if (resp == null) return;
                }
                Member leader = resp.getLeader();
                List<String> leaderUrls = leader.getClientUrlsList();
                if (leaderUrls.isEmpty()) return;
                leaderUrlStr = leaderUrls.get(0);
                // TODO: Why not strip protocol info on server side since grpc does not need it
                URL tURL = new URL(leaderUrlStr);
                HostAndPort newLeader = HostAndPort.fromParts(tURL.getHost(), tURL.getPort());
                if (leaderWrapper != null && newLeader.equals(leaderWrapper.getLeaderInfo())) {
                    return;
                }

                // switch leader
                ManagedChannel clientChannel = getManagedChannel(newLeader);
                leaderWrapper = new LeaderWrapper(newLeader,
                        PDGrpc.newBlockingStub(clientChannel),
                        PDGrpc.newStub(clientChannel),
                        clientChannel,
                        System.nanoTime());
                logger.info("Switched to new leader: %s", newLeader.toString());
            }
        } catch (MalformedURLException e) {
            logger.error("Client URL is not valid: %s", leaderUrlStr, e);
        } catch (Exception e) {
            logger.error("Error updating leader.", e);
        }
    }

    @Override
    protected PDBlockingStub getBlockingStub() {
        return leaderWrapper.getBlockingStub()
                            .withDeadlineAfter(getConf().getTimeout(),
                                               getConf().getTimeoutUnit());
    }

    @Override
    protected PDStub getAsyncStub() {
        return leaderWrapper.getAsyncStub()
                            .withDeadlineAfter(getConf().getTimeout(),
                                               getConf().getTimeoutUnit());
    }

    private PDClient(TiSession session) {
        super(session);
    }

    private void initCluster() {
        GetMembersResponse resp = getMembers();
        checkNotNull(resp, "Failed to init client for PD cluster.");
        long clusterId = resp.getHeader().getClusterId();
        header = RequestHeader.newBuilder().setClusterId(clusterId).build();
        tsoReq = TsoRequest.newBuilder().setHeader(header).build();
        updateLeader(resp);
        service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(() -> updateLeader(null),
                                    1, 1, TimeUnit.MINUTES);
    }

    static PDClient createRaw(TiSession session) {
        PDClient client = null;
        try {
            client = new PDClient(session);
            client.initCluster();
        } catch (Exception e) {
            if (client != null) {
                try {
                    client.close();
                } catch (InterruptedException ignore) {
                }
            }
        }

        return client;
    }
}
