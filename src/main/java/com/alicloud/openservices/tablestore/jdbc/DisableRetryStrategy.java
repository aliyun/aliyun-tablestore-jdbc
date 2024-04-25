package com.alicloud.openservices.tablestore.jdbc;

import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class DisableRetryStrategy implements RetryStrategy {
    @Override
    public RetryStrategy clone() {
        return new DisableRetryStrategy();
    }

    @Override
    public int getRetries() {
        return 0;
    }

    @Override
    public long nextPause(String s, Exception e) {
        return 0;
    }
}
