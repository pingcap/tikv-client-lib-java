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

import com.pingcap.tikv.policy.RetryNTimes;
import com.pingcap.tikv.policy.RetryPolicy;

/**
 * NOT thread-safe!! A session suppose to be change by single thread in master node and use by
 * slaves for read only purpose
 */
public class TiSession {
  private static final RetryPolicy.Builder DEF_RETRY_POLICY_BUILDER = new RetryNTimes.Builder(3);

  private TiConfiguration conf;
  private RetryPolicy.Builder retryPolicyBuilder = DEF_RETRY_POLICY_BUILDER;

  public TiSession(TiConfiguration conf) {
    this.conf = conf;
  }

  public TiConfiguration getConf() {
    return conf;
  }

  public static TiSession create(TiConfiguration conf) {
    return new TiSession(conf);
  }

  RetryPolicy.Builder getRetryPolicyBuilder() {
    return retryPolicyBuilder;
  }
}
