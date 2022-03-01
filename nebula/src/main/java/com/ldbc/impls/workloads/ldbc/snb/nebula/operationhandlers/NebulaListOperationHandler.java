package com.ldbc.impls.workloads.ldbc.snb.nebula.operationhandlers;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.ResultReporter;
import com.ldbc.impls.workloads.ldbc.snb.nebula.NebulaDbConnectionState;
import com.ldbc.impls.workloads.ldbc.snb.operationhandlers.ListOperationHandler;
import com.vesoft.nebula.client.graph.data.ResultSet;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.List;

public abstract class NebulaListOperationHandler <TOperation extends Operation<List<TOperationResult>>, TOperationResult>
        implements ListOperationHandler<TOperationResult, TOperation, NebulaDbConnectionState> {
    @Override
    public void executeOperation(TOperation operation, NebulaDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        // TODO:
    }

    public abstract TOperationResult convertSingleResult(ResultSet.Record record) throws UnsupportedEncodingException;

}
