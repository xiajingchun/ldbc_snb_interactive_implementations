package com.ldbc.impls.workloads.ldbc.snb.nebula;

import com.ldbc.driver.DbException;
import com.ldbc.impls.workloads.ldbc.snb.QueryStore;

public class NebulaQueryStore extends QueryStore {
    public NebulaQueryStore(String path, String postfix) throws DbException {
        super(path, ".nebula");
    }
}
