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
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.concurrent.Callable;

public abstract class RetryPolicy {
    private static final Logger logger = LogManager.getFormatterLogger(RetryPolicy.class);

    // Basically a leader recheck method
    private Callable<Void> recoverMethod;

    private ImmutableSet<Status> unrecoverableStatus = ImmutableSet.of(
            Status.ALREADY_EXISTS, Status.PERMISSION_DENIED,
            Status.INVALID_ARGUMENT, Status.NOT_FOUND,
            Status.UNIMPLEMENTED, Status.OUT_OF_RANGE,
            Status.UNAUTHENTICATED
    );

    public RetryPolicy(Callable<Void> recoverMethod) {
        this.recoverMethod = recoverMethod;
    }

    protected abstract boolean shouldRetry(Exception e);

    protected boolean checkNotLeaderException(Status status) {
        // TODO: need a way to check this, for now all unknown exception
        return true;
    }

    protected boolean checkNotRecoverableException(Status status) {
        return unrecoverableStatus.contains(status);
    }

    public <T> T callWithRetry(Callable<T> proc, String methodName) {
        while (true) {
            try {
                T result = proc.call();
                return result;
            } catch (Exception e) {
                Status status = Status.fromThrowable(e);
                if (checkNotRecoverableException(status) || !shouldRetry(e)) {
                    logger.error("Failed to recover from last grpc error calling %s.", methodName);
                    throw new GrpcException(e);
                }
                try {
                    if (recoverMethod != null && checkNotLeaderException(status)) {
                        logger.info("Leader switched, recovering...");
                        recoverMethod.call();
                    }
                } catch (Exception e1) {
                    // Ignore exception further spreading
                    logger.error("Error during grpc leader update.", e1);
                }
            }
        }
    }

    public interface Builder {
        RetryPolicy create(Callable<Void> recoverMethod);
    }
}
