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
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.kvproto.Kvrpcpb.IsolationLevel;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.kvproto.PDGrpc;
import com.pingcap.tikv.kvproto.PDGrpc.PDBlockingStub;
import com.pingcap.tikv.kvproto.PDGrpc.PDStub;
import com.pingcap.tikv.kvproto.Pdpb.*;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.operation.PDErrorHandler;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.types.BytesType;
import com.pingcap.tikv.util.FutureObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class PDClient extends AbstractGrpcClient<PDBlockingStub, PDStub>
    implements ReadOnlyPDClient {
  private RequestHeader header;
  private TsoRequest tsoReq;
  private volatile LeaderWrapper leaderWrapper;
  private ScheduledExecutorService service;
  private IsolationLevel isolationLevel;

  @Override
  public TiTimestamp getTimestamp() {
    // TODO: check with Xiaoyu. This may be problematic since it is only test with PDClient.
    // https://github.com/pingcap/pd/blob/master/pd-client/client.go#L284
    // PD's implementation has a tsLoop which is bi-directional steam, but our impl is just call
    // and call onComplete immediately after calling onNext. In other word, we only calling onNext one time which is
    // not semantic correct if we are speaking bi-directional stream here.
    // Bidirectional streaming RPCs where both sides send a sequence of messages using a read-write stream.
    FutureObserver<Timestamp, TsoResponse> responseObserver =
        new FutureObserver<>(TsoResponse::getTimestamp);
    // Problem 1. Casting error if we take the following approach:
    // If we store resp in FutureObserver
    // PDErrorHandler<FutureObserver<GetRegionResponse, Timestamp>> handler = new PDErrorHandler<>(f -> f.getResp().getHead().getError());
    // Since correctness of current impl remain unsure and we did not use any getTimestamp in our codebase. We simply supply a null
    // error handler to bypass the method signature.
    StreamObserver<TsoRequest> requestObserver =
        callBidiStreamingWithRetry(PDGrpc.METHOD_TSO, responseObserver, null);
    requestObserver.onNext(tsoReq);
    requestObserver.onCompleted();
    try {
      Timestamp timestamp = responseObserver.getFuture().get();
      return new TiTimestamp(timestamp.getPhysical(), timestamp.getLogical());
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      throw new GrpcException(e);
    }
    return null;
  }

  @Override
  public TiRegion getRegionByKey(ByteString key) {
    CodecDataOutput cdo = new CodecDataOutput();
    BytesType.writeBytes(cdo, key.toByteArray());
    ByteString encodedKey = cdo.toByteString();

    GetRegionRequest request =
        GetRegionRequest.newBuilder().setHeader(header).setRegionKey(encodedKey).build();

    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(r -> r.getHeader().hasError() ? r.getHeader().getError() : null, this);

    GetRegionResponse resp = callWithRetry(PDGrpc.METHOD_GET_REGION, request, handler);
    return new TiRegion(resp.getRegion(), resp.getLeader(), isolationLevel);
  }

  @Override
  public Future<TiRegion> getRegionByKeyAsync(ByteString key) {
    FutureObserver<TiRegion, GetRegionResponse> responseObserver =
        new FutureObserver<>(resp -> new TiRegion(resp.getRegion(), resp.getLeader(), isolationLevel));
    GetRegionRequest request =
        GetRegionRequest.newBuilder().setHeader(header).setRegionKey(key).build();

    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(r -> r.getHeader().hasError() ? r.getHeader().getError() : null, this);
    callAsyncWithRetry(PDGrpc.METHOD_GET_REGION, request, responseObserver, handler);
    return responseObserver.getFuture();
  }

  @Override
  public TiRegion getRegionByID(long id) {
    GetRegionByIDRequest request =
        GetRegionByIDRequest.newBuilder().setHeader(header).setRegionId(id).build();
    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(r -> r.getHeader().hasError() ? r.getHeader().getError() : null, this);
    GetRegionResponse resp = callWithRetry(PDGrpc.METHOD_GET_REGION_BY_ID, request, handler);
    // Instead of using default leader instance, explicitly set no leader to null
    return new TiRegion(resp.getRegion(), resp.getLeader(), isolationLevel);
  }

  /**
   * Change default read committed to other isolation level.
   * @param level is a enum which indicates isolation level.
   */
  private void setIsolationLevel(IsolationLevel level) {
    this.isolationLevel = level;
  }

  @Override
  public Future<TiRegion> getRegionByIDAsync(long id) {
    FutureObserver<TiRegion, GetRegionResponse> responseObserver =
        new FutureObserver<>(resp -> new TiRegion(resp.getRegion(), resp.getLeader(), isolationLevel));

    GetRegionByIDRequest request =
        GetRegionByIDRequest.newBuilder().setHeader(header).setRegionId(id).build();
    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(r -> r.getHeader().hasError() ? r.getHeader().getError() : null, this);
    callAsyncWithRetry(PDGrpc.METHOD_GET_REGION_BY_ID, request, responseObserver, handler);
    return responseObserver.getFuture();
  }

  @Override
  public Store getStore(long storeId) {
    GetStoreRequest request =
        GetStoreRequest.newBuilder().setHeader(header).setStoreId(storeId).build();
    PDErrorHandler<GetStoreResponse> handler =
        new PDErrorHandler<>(r -> r.getHeader().hasError() ? r.getHeader().getError() : null, this);
    GetStoreResponse resp = callWithRetry(PDGrpc.METHOD_GET_STORE, request, handler);
    Store store = resp.getStore();
    if (store.getState() == Metapb.StoreState.Tombstone) {
      return null;
    }
    return store;
  }

  @Override
  public Future<Store> getStoreAsync(long storeId) {
    FutureObserver<Store, GetStoreResponse> responseObserver =
        new FutureObserver<>(
            (GetStoreResponse resp) -> {
              Store store = resp.getStore();
              if (store.getState() == Metapb.StoreState.Tombstone) {
                return null;
              }
              return store;
            });

    GetStoreRequest request =
        GetStoreRequest.newBuilder().setHeader(header).setStoreId(storeId).build();
    PDErrorHandler<GetStoreResponse> handler =
        new PDErrorHandler<>(r -> r.getHeader().hasError() ? r.getHeader().getError() : null, this);
    callAsyncWithRetry(PDGrpc.METHOD_GET_STORE, request, responseObserver, handler);
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

  @VisibleForTesting
  RequestHeader getHeader() {
    return header;
  }

  @VisibleForTesting
  LeaderWrapper getLeaderWrapper() {
    return leaderWrapper;
  }

  class LeaderWrapper {
    private final HostAndPort leaderInfo;
    private final PDBlockingStub blockingStub;
    private final PDStub asyncStub;
    private final ManagedChannel channel;
    private final long createTime;

    LeaderWrapper(
        HostAndPort leaderInfo,
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

    HostAndPort getLeaderInfo() {
      return leaderInfo;
    }

    PDBlockingStub getBlockingStub() {
      return blockingStub;
    }

    PDStub getAsyncStub() {
      return asyncStub;
    }

    long getCreateTime() {
      return createTime;
    }

    void close() {
      channel.shutdown();
    }
  }

  private ManagedChannel getManagedChannel(HostAndPort url) {
    return ManagedChannelBuilder.forAddress(url.getHostText(), url.getPort())
        .usePlaintext(true)
        .build();
  }

  public GetMembersResponse getMembers() {
    List<HostAndPort> pdAddrs = getConf().getPdAddrs();
    checkArgument(pdAddrs.size() > 0, "No PD address specified.");
    for (HostAndPort url : pdAddrs) {
      ManagedChannel probChan = null;
      try {
        probChan = getManagedChannel(url);
        PDGrpc.PDBlockingStub stub = PDGrpc.newBlockingStub(probChan);
        GetMembersRequest request =
            GetMembersRequest.newBuilder().setHeader(RequestHeader.getDefaultInstance()).build();
        return stub.getMembers(request);
      } catch (Exception ignore) {
      } finally {
        if (probChan != null) {
          probChan.shutdownNow();
        }
      }
    }
    return null;
  }

  public void updateLeader(GetMembersResponse resp) {
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
        leaderWrapper =
            new LeaderWrapper(
                newLeader,
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
    return leaderWrapper
        .getBlockingStub()
        .withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
  }

  @Override
  protected PDStub getAsyncStub() {
    return leaderWrapper
        .getAsyncStub()
        .withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
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
    service.scheduleAtFixedRate(() -> updateLeader(null), 1, 1, TimeUnit.MINUTES);
  }

  public static PDClient createRaw(TiSession session) {
    PDClient client = null;
    try {
      client = new PDClient(session);
      client.setIsolationLevel(IsolationLevel.RC);
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
