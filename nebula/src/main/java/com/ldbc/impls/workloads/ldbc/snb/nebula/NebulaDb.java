package com.ldbc.impls.workloads.ldbc.snb.nebula;

import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.impls.workloads.ldbc.snb.db.BaseDb;

import java.util.Map;

public class NebulaDb  extends BaseDb<NebulaQueryStore> {
    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        // TODO
    }
}
