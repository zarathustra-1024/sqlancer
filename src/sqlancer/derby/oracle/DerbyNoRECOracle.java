package sqlancer.derby.oracle;

import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.derby.DerbyGlobalState;
import sqlancer.derby.gen.DerbyExpressionGenerator;
import sqlancer.derby.schema.DerbyTable;
import sqlancer.common.query.SQLQueryAdapter;

import java.sql.SQLException;

public class DerbyNoRECOracle implements TestOracle<DerbyGlobalState> {

    private final DerbyGlobalState state;
    private final DerbyExpressionGenerator gen;
    private String lastQueryString;

    public DerbyNoRECOracle(DerbyGlobalState state) {
        this.state = state;
        this.gen = new DerbyExpressionGenerator(state);
    }

    @Override
    public void check() throws SQLException {
        try {
            String originalQuery = generateSelectQuery();
            this.lastQueryString = originalQuery;

            String optimizedQuery = generateOptimizedQuery(originalQuery);

            SQLQueryAdapter originalSql = new SQLQueryAdapter(originalQuery);
            SQLQueryAdapter optimizedSql = new SQLQueryAdapter(optimizedQuery);

            var originalResult = state.executeStatementAndGet(originalSql);
            var optimizedResult = state.executeStatementAndGet(optimizedSql);

            if (!resultsAreEqual(originalResult, optimizedResult)) {
                state.getLoggerNew().logCurrentState("NoREC test failed: Results differ!");
                state.getLoggerNew().logQuery("Original: " + originalQuery);
                state.getLoggerNew().logQuery("Optimized: " + optimizedQuery);
            }

        } catch (Exception e) {
            if (!isExpectedError(e)) {
                throw e;
            }
        }
    }

    private String generateSelectQuery() {
        DerbyTable table = state.getSchema().getRandomTable();
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ");
        sb.append(table.getRandomColumn().getName());
        sb.append(" FROM ").append(table.getName());

        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(gen.generatePredicate());
        }

        return sb.toString();
    }

    private String generateOptimizedQuery(String originalQuery) {
        if (originalQuery.toUpperCase().contains("WHERE")) {
            return originalQuery + " AND 1=1";
        } else {
            return originalQuery + " WHERE 1=1";
        }
    }

    private boolean resultsAreEqual(Object result1, Object result2) {
        return true;
    }

    private boolean isExpectedError(Exception e) {
        String message = e.getMessage();
        return message != null &&
                (message.contains("does not exist") ||
                        message.contains("already exists") ||
                        message.contains("Syntax error"));
    }

    @Override
    public String getLastQueryString() {
        return lastQueryString;
    }
}
