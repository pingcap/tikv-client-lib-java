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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.KeyRange;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.grpc.*;
import com.pingcap.tikv.grpc.Errorpb.Error;
import com.pingcap.tikv.grpc.Kvrpcpb.Context;
import com.pingcap.tikv.util.TiFluentIterable;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;


public class KVMockServer extends TikvGrpc.TikvImplBase {

    public int port;
    public Server server;
    private Metapb.Region region;
    private TreeMap<String, String> dataMap = new TreeMap<>();
    private Map<ByteString, Integer> errorMap = new HashMap<>();

    public static final int ABORT = 1;
    public static final int RETRY = 2;

    public void put(String key, String value) {
        dataMap.put(key, value);
    }
    public void putError(String key, int code) {
        errorMap.put(ByteString.copyFromUtf8(key), code);
    }

    public void clearAllMap() {
        dataMap.clear();
        errorMap.clear();
    }

    private void verifyContext(Context context) throws Exception {
        if (context.getRegionId() != region.getId() ||
                !context.getRegionEpoch().equals(region.getRegionEpoch()) ||
                !context.getPeer().equals(region.getPeers(0))) {
            throw new Exception();
        }
    }

    @Override
    public void kvGet(com.pingcap.tikv.grpc.Kvrpcpb.GetRequest request,
                      io.grpc.stub.StreamObserver<com.pingcap.tikv.grpc.Kvrpcpb.GetResponse> responseObserver) {
        try {
            verifyContext(request.getContext());
            if (request.getVersion() == 0) {
                throw new Exception();
            }
            ByteString key = request.getKey();


            Kvrpcpb.GetResponse.Builder builder = Kvrpcpb.GetResponse.newBuilder();
            Integer errorCode = errorMap.get(key);
            Kvrpcpb.KeyError.Builder errBuilder = Kvrpcpb.KeyError.newBuilder();
            if (errorCode != null) {
                if (errorCode == ABORT) {
                    errBuilder.setAbort("ABORT");
                } else if (errorCode == RETRY) {
                    errBuilder.setRetryable("Retry");
                }
                builder.setError(errBuilder);
            } else {
                ByteString value = ByteString.copyFromUtf8(dataMap.get(key.toStringUtf8()));
                builder.setValue(value);
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    public void kvScan(com.pingcap.tikv.grpc.Kvrpcpb.ScanRequest request,
                       io.grpc.stub.StreamObserver<com.pingcap.tikv.grpc.Kvrpcpb.ScanResponse> responseObserver) {
        try {
            verifyContext(request.getContext());
            if (request.getVersion() == 0) {
                throw new Exception();
            }
            ByteString key = request.getStartKey();

            Kvrpcpb.ScanResponse.Builder builder = Kvrpcpb.ScanResponse.newBuilder();
            Error.Builder errBuilder = Error.newBuilder();
            Integer errorCode = errorMap.get(key);
            if (errorCode != null) {
                if (errorCode == ABORT) {
                    errBuilder.setServerIsBusy(Errorpb.ServerIsBusy.getDefaultInstance());
                }
                builder.setRegionError(errBuilder.build());
            } else {
                ByteString startKey = request.getStartKey();
                SortedMap<String, String> kvs = dataMap.tailMap(startKey.toStringUtf8());
                builder.addAllPairs(Iterables.transform(kvs.entrySet(),
                                                        kv -> Kvrpcpb.KvPair.newBuilder()
                                                                            .setKey(ByteString.copyFromUtf8(kv.getKey()))
                                                                            .setValue(ByteString.copyFromUtf8(kv.getValue()))
                                                                            .build()));
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    public void kvBatchGet(com.pingcap.tikv.grpc.Kvrpcpb.BatchGetRequest request,
                           io.grpc.stub.StreamObserver<com.pingcap.tikv.grpc.Kvrpcpb.BatchGetResponse> responseObserver) {
        try {
            verifyContext(request.getContext());
            if (request.getVersion() == 0) {
                throw new Exception();
            }
            List<ByteString> keys = request.getKeysList();

            Kvrpcpb.BatchGetResponse.Builder builder = Kvrpcpb.BatchGetResponse.newBuilder();
            Error.Builder errBuilder = Error.newBuilder();
            ImmutableList.Builder<Kvrpcpb.KvPair> resultList = ImmutableList.builder();
            for (ByteString key : keys) {
                Integer errorCode = errorMap.get(key);
                if (errorCode != null) {
                    if (errorCode == ABORT) {
                        errBuilder.setServerIsBusy(Errorpb.ServerIsBusy.getDefaultInstance());
                    }
                    builder.setRegionError(errBuilder.build());
                    break;
                } else {
                    ByteString value = ByteString.copyFromUtf8(dataMap.get(key.toStringUtf8()));
                    resultList.add(Kvrpcpb.KvPair.newBuilder().setKey(key)
                                                              .setValue(value)
                                                              .build());
                }
            }
            builder.addAllPairs(resultList.build());
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    public void coprocessor(com.pingcap.tikv.grpc.Coprocessor.Request requestWrap,
                            io.grpc.stub.StreamObserver<com.pingcap.tikv.grpc.Coprocessor.Response> responseObserver) {
        try {
            verifyContext(requestWrap.getContext());

            SelectRequest request = SelectRequest.parseFrom(requestWrap.getData());
            if (request.getStartTs() == 0) {
                throw new Exception();
            }
            List<Coprocessor.KeyRange> wrapRanges = requestWrap.getRangesList();
            List<KeyRange> keyRanges = request.getRangesList();

            for (Coprocessor.KeyRange range : wrapRanges) {
                if (!Iterables.any(keyRanges,
                        innerRange -> innerRange.getLow().equals(range.getStart()) &&
                                      innerRange.getHigh().equals(range.getEnd()))) {
                    throw new Exception();
                }
            }

            Coprocessor.Response.Builder builderWrap = Coprocessor.Response.newBuilder();
            SelectResponse.Builder builder = SelectResponse.newBuilder();
            com.pingcap.tidb.tipb.Error.Builder errBuilder = com.pingcap.tidb.tipb.Error.newBuilder();

            for (KeyRange keyRange : keyRanges) {
                Integer errorCode = errorMap.get(keyRange.getLow());
                if (errorCode != null) {
                    if (errorCode == ABORT) {
                        errBuilder.setCode(errorCode);
                        errBuilder.setMsg("whatever");
                    }
                    builder.setError(errBuilder.build());
                    break;
                } else {
                    ByteString startKey = keyRange.getLow();
                    SortedMap<String, String> kvs = dataMap.tailMap(startKey.toStringUtf8());
                    builder.addAllChunks(TiFluentIterable.from(kvs.entrySet())
                            .stopWhen(kv -> kv.getKey().compareTo(keyRange.getHigh().toStringUtf8()) > 0)
                            .transform(kv -> Chunk.newBuilder()
                                    .setRowsData(ByteString.copyFromUtf8(kv.getValue()))
                                    .build()));
                }
            }

            responseObserver.onNext(builderWrap.setData(builder.build()
                                                        .toByteString())
                                               .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    public int start(Metapb.Region region) throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        server = ServerBuilder.forPort(port)
                .addService(this)
                .build()
                .start();

        this.region = region;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> KVMockServer.this.stop()));
        return port;
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
}
