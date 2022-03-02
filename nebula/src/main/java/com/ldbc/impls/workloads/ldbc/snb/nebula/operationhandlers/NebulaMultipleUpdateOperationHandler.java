package com.ldbc.impls.workloads.ldbc.snb.nebula.operationhandlers;

import com.ldbc.driver.*;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.impls.workloads.ldbc.snb.nebula.NebulaDbConnectionState;
import com.ldbc.impls.workloads.ldbc.snb.operationhandlers.MultipleUpdateOperationHandler;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.Session;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;



public abstract class NebulaMultipleUpdateOperationHandler<TOperation extends Operation<LdbcNoResult>>
        implements MultipleUpdateOperationHandler<TOperation, NebulaDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation, NebulaDbConnectionState state, ResultReporter resultReporter) throws DbException {
        Session session = state.getSession();
        try {
            List<String> queryStrings = getQueryString(state, operation);
            for (String queryString : queryStrings) {
                session.execute(queryString);
            }
        } catch (Exception e) {
            throw new DbException(e);
        }
    }

}