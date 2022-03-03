package com.ldbc.impls.workloads.ldbc.snb.nebula.operationhandlers;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.ResultReporter;
import com.ldbc.impls.workloads.ldbc.snb.nebula.NebulaDbConnectionState;
import com.ldbc.impls.workloads.ldbc.snb.operationhandlers.SingletonOperationHandler;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.Session;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;

public abstract class NebulaSingletonOperationHandler <TOperation extends Operation<TOperationResult>, TOperationResult>
        implements SingletonOperationHandler<TOperationResult, TOperation, NebulaDbConnectionState> {
    @Override
    public void executeOperation(TOperation operation, NebulaDbConnectionState state, ResultReporter resultReporter) throws DbException {
        Session session = state.getSession();
        try {
            TOperationResult tuple = null;
            int resultCount = 0;

            final String queryString = getQueryString(state, operation);
            state.logQuery(operation.getClass().getSimpleName(), queryString);
            final ResultSet result = session.execute(queryString);
            if (!result.isSucceeded()) {
                resultReporter.report(0, noResult(), operation);
                return;
            }
            if (result.rowsSize() > 0) {
                final ResultSet.Record record = result.rowValues(0);
                resultCount++;

                tuple = convertSingleResult(record);

                if (state.isPrintResults()) {
                    System.out.println(tuple.toString());
                }
            }

            resultReporter.report(resultCount, tuple, operation);
        } catch (Exception e) {
            throw new DbException(e);
        }
    }

    public abstract TOperationResult convertSingleResult(ResultSet.Record record) throws UnsupportedEncodingException, ParseException;

    public abstract TOperationResult  noResult();
}
