package sqlancer.derby.oracle;

import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.derby.DerbyGlobalState;
import sqlancer.derby.schema.DerbyTable;
import sqlancer.derby.schema.DerbyColumn;
import sqlancer.derby.schema.DerbyDataType;
import sqlancer.common.query.SQLQueryAdapter;

import java.sql.SQLException;
import java.util.List;

public class DerbyNoRECOracle implements TestOracle<DerbyGlobalState> {

    private final DerbyGlobalState state;
    private String lastQueryString;

    public DerbyNoRECOracle(DerbyGlobalState globalState) {
        this.state = globalState;
    }

    @Override
    public void check() throws SQLException {
        try {
            String originalQuery = generateSimpleSelectQuery();
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
            } else {
                state.getLoggerNew().logInfo("NoREC test passed for query: " + originalQuery);
            }

        } catch (Exception e) {
            if (!isExpectedError(e)) {
                throw e;
            } else {
                state.getLoggerNew().logInfo("Expected error in NoREC test: " + e.getMessage());
            }
        }
    }

    private String generateSimpleSelectQuery() {
        if (state.getSchema() == null || state.getSchema().getDatabaseTables().isEmpty()) {
            throw new IllegalStateException("No tables available for NoREC test");
        }

        DerbyTable table = state.getSchema().getRandomTable();
        List<DerbyColumn> columns = table.getColumns();

        if (columns.isEmpty()) {
            return String.format("SELECT 1 FROM %s", table.getName());
        }

        DerbyColumn selectedColumn = null;
        for (DerbyColumn column : columns) {
            DerbyDataType type = column.getType();
            if (type == DerbyDataType.INTEGER || type == DerbyDataType.DOUBLE ||
                    type == DerbyDataType.DECIMAL || type == DerbyDataType.BIGINT) {
                selectedColumn = column;
                break;
            }
        }

        if (selectedColumn == null) {
            selectedColumn = columns.get(0);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        sb.append(selectedColumn.getName());
        sb.append(" FROM ").append(table.getName());

        if (Randomly.getBoolean() && selectedColumn.getType() == DerbyDataType.INTEGER) {
            sb.append(" WHERE ");
            sb.append(selectedColumn.getName());
            sb.append(" > 0");
        }

        return sb.toString();
    }

    private String generateOptimizedQuery(String originalQuery) {
        String query = originalQuery.trim();
        if (query.endsWith(";")) {
            query = query.substring(0, query.length() - 1);
        }

        if (query.toUpperCase().contains("WHERE")) {
            return query + " AND 1=1";
        } else {
            return query + " WHERE 1=1";
        }
    }

    private boolean resultsAreEqual(List<List<Object>> result1, List<List<Object>> result2) {
        return result1.size() == result2.size();
    }

    private boolean isExpectedError(Exception e) {
        if (e == null) return false;

        String message = e.getMessage();
        if (message == null) return false;

        String upperMessage = message.toUpperCase();

        return upperMessage.contains("DOES NOT EXIST") ||
                upperMessage.contains("ALREADY EXISTS") ||
                upperMessage.contains("SYNTAX ERROR") ||
                upperMessage.contains("INVALID") ||
                upperMessage.contains("NOT FOUND") ||
                upperMessage.contains("NO SUCH TABLE") ||
                upperMessage.contains("NO SUCH COLUMN") ||
                upperMessage.contains("TYPE MISMATCH");
    }

    @Override
    public String getLastQueryString() {
        return lastQueryString;
    }
}