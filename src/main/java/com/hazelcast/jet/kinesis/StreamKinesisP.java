package com.hazelcast.jet.kinesis;

import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class StreamKinesisP extends AbstractProcessor {

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        return false;
    }

    @Override
    public void close(@Nullable Throwable error) throws Exception {

    }
}
