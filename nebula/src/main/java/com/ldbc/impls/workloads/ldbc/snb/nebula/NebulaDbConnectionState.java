package com.ldbc.impls.workloads.ldbc.snb.nebula;

import com.ldbc.impls.workloads.ldbc.snb.BaseDbConnectionState;

import java.io.IOException;
import java.util.Map;

public class NebulaDbConnectionState  extends BaseDbConnectionState<NebulaQueryStore> {
    public NebulaDbConnectionState(Map<String, String> properties, NebulaQueryStore queryStore) {
        super(properties, queryStore);

        // TODO:
    }

    @Override
    public void close() throws IOException {
        // TODO:
    }
}
