package com.ldbc.impls.workloads.ldbc.snb.postgres.operationhandlers;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.ResultReporter;
import com.ldbc.impls.workloads.ldbc.snb.operationhandlers.SingletonOperationHandler;
import com.ldbc.impls.workloads.ldbc.snb.postgres.PostgresDbConnectionState;

import java.sql.*;

public abstract class PostgresSingletonOperationHandler<TOperation extends Operation<TOperationResult>, TOperationResult>
        extends PostgresOperationHandler
        implements SingletonOperationHandler<TOperationResult, TOperation, PostgresDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation, PostgresDbConnectionState state,
                                 ResultReporter resultReporter) throws DbException {
        Connection conn = state.getConnection();
        TOperationResult tuple = null;
        int resultCount = 0;

        String queryString = getQueryString(state, operation);
        replaceParameterNamesWithQuestionMarks(operation, queryString);

        try (final PreparedStatement stmt = prepareStatement(operation, conn)) {
            state.logQuery(operation.getClass().getSimpleName(), queryString);

            System.out.println("- executing query . . . ");
            ResultSet result = stmt.executeQuery();
            System.out.println("- fetching results . . . ");
            if (result.next()) {
                System.out.println("- next result . . . ");
                resultCount++;

                tuple = convertSingleResult(result);
                if (state.isPrintResults()) {
                    System.out.println(tuple.toString());
                }
            }
        } catch (Exception e) {
            throw new DbException(e);
        }
        resultReporter.report(resultCount, tuple, operation);
    }

    public abstract TOperationResult convertSingleResult(ResultSet result) throws SQLException;
}
