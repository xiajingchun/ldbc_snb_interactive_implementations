package com.ldbc.impls.workloads.ldbc.snb.nebula.interactive;

import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.impls.workloads.ldbc.snb.nebula.NebulaDb;

import java.util.Map;

public class NebulaInteractiveDb extends NebulaDb {
    // TODO:
    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        super.onInit(properties, loggingService);
        registerOperationHandler(LdbcShortQuery1PersonProfile.class, ShortQuery1PersonProfile.class);
    }
}
