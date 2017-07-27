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

package com.pingcap.tikv.policy;

import com.google.common.collect.ImmutableSet;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.operation.ErrorHandler;
import io.grpc.Status;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class RetryPolicy {
  private static final Logger logger = LogManager.getFormatterLogger(RetryPolicy.class);

  // Basically a leader recheck method
  private ErrorHandler handler;

  private ImmutableSet<Status.Code> unrecoverableStatus =
      ImmutableSet.of(
          Status.Code.ALREADY_EXISTS, Status.Code.PERMISSION_DENIED,
          Status.Code.INVALID_ARGUMENT, Status.Code.NOT_FOUND,
          Status.Code.UNIMPLEMENTED, Status.Code.OUT_OF_RANGE,
          Status.Code.UNAUTHENTICATED, Status.Code.CANCELLED);

  public RetryPolicy(ErrorHandler handler) {
    this.handler = handler;
  }

  protected abstract boolean shouldRetry(Exception e);

  protected boolean checkNotLeaderException(Status status) {
    // TODO: need a way to check this, for now all unknown exception
    return true;
  }

  protected boolean checkNotRecoverableException(Status status) {
    return unrecoverableStatus.contains(status.getCode());
  }

  @SuppressWarnings("unchecked")
  public <T> T callWithRetry(Callable<T> proc, String methodName) {
    while (true) {
      try {
        T result = proc.call();
        // TODO: null check is only for temporary. In theory, every rpc call need
        // have some mechanism to retry call. The reason we allow this having two reason:
        // 1. Test's resp is null
        // 2. getTimestamp pass a null error handler for now, since impl of it is not correct yet.
        if (handler != null) {
          handler.handle(result);
        }
        return result;
      } catch (Exception e) {
        // TODO retry is keep sending request to server, this is really bad behavior here. More refractory on the
        // way
        Status status = Status.fromThrowable(e);
        if (checkNotRecoverableException(status) || !shouldRetry(e)) {
          logger.error("Failed to recover from last grpc error calling %s.", methodName);
          throw new GrpcException(e);
        }
      }
    }
  }

  public interface Builder {
    RetryPolicy create(ErrorHandler handler);
  }
}
