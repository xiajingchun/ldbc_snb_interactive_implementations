package com.ldbc.impls.workloads.ldbc.snb.nebula.operationhandlers;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import com.ldbc.impls.workloads.ldbc.snb.nebula.NebulaDbConnectionState;
import com.ldbc.impls.workloads.ldbc.snb.operationhandlers.UpdateOperationHandler;
import com.vesoft.nebula.client.graph.net.Session;

public abstract class NebulaUpdateOperationHandler <TOperation extends Operation<LdbcNoResult>>
        implements UpdateOperationHandler<TOperation, NebulaDbConnectionState> {
    @Override
    public void executeOperation(TOperation operation, NebulaDbConnectionState state, ResultReporter resultReporter) throws DbException {
        Session session = state.getSession();
//        if (operation instanceof LdbcUpdate1AddPerson) {
//            // TODO:
//        } else if (operation instanceof LdbcUpdate4AddForum) {
//            // TODO:
//        } else if (operation instanceof LdbcUpdate6AddPost) {
//            // TODO:
//        } else if (operation instanceof LdbcUpdate8AddFriendship) {
//            // TODO:
//        } else {
            try {
                final String queryString = getQueryString(state, operation);
                state.logQuery(operation.getClass().getSimpleName(), queryString);
                session.execute(queryString);
            } catch (Exception e) {
                throw new DbException(e);
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
}
