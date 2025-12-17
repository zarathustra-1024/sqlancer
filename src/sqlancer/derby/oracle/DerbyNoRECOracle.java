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
            var originalQuery = generateSimpleSelectQuery();
            this.lastQueryString = originalQuery;

            var optimizedQuery = generateOptimizedQuery(originalQuery);

            var originalSql = new SQLQueryAdapter(originalQuery);
            var optimizedSql = new SQLQueryAdapter(optimizedQuery);

            // 记录查询到控制台
            state.getLoggerNew().logQuery("NoREC test query: " + originalQuery);

            var originalResult = state.executeStatementAndGet(originalSql);
            var optimizedResult = state.executeStatementAndGet(optimizedSql);

            if (!resultsAreEqual(originalResult, optimizedResult)) {
                // 记录错误到文件（包含DQL）
                state.getLoggerNew().logError("NoREC test failed: Results differ!", originalQuery);
                state.getLoggerNew().logQuery("Original: " + originalQuery);
                state.getLoggerNew().logQuery("Optimized: " + optimizedQuery);
            } else {
                // 成功信息只输出到控制台
                state.getLoggerNew().logInfo("NoREC test passed for query: " + originalQuery);
            }

        } catch (Exception e) {
            if (!isExpectedError(e)) {
                // 记录异常和DQL到文件
                if (lastQueryString != null) {
                    state.getLoggerNew().logException(e, lastQueryString);
                } else {
                    state.getLoggerNew().logException(e);
                }
                throw e;
            } else {
                // 预期错误也记录到文件
                var errorMsg = e.getMessage() != null ? e.getMessage() : e.toString();
                if (lastQueryString != null) {
                    state.getLoggerNew().logError("Expected error in NoREC test: " + errorMsg, lastQueryString);
                } else {
                    // 如果没有DQL，只输出到控制台
                    state.getLoggerNew().logInfo("Expected error in NoREC test: " + errorMsg);
                }
            }
        }
    }

    private String generateSimpleSelectQuery() {
        if (state.getSchema() == null || state.getSchema().getDatabaseTables().isEmpty()) {
            throw new IllegalStateException("No tables available for NoREC test");
        }

        var table = state.getSchema().getRandomTable();
        var columns = table.getColumns();

        if (columns.isEmpty()) {
            return String.format("SELECT 1 FROM %s", table.getName());
        }

        DerbyColumn selectedColumn = null;
        for (var column : columns) {
            var type = column.getType();
            if (type == DerbyDataType.INTEGER || type == DerbyDataType.DOUBLE ||
                    type == DerbyDataType.DECIMAL || type == DerbyDataType.BIGINT) {
                selectedColumn = column;
                break;
            }
        }

        if (selectedColumn == null) {
            selectedColumn = columns.get(0);
        }

        var sb = new StringBuilder();
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
        var query = originalQuery.trim();
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

        var message = e.getMessage();
        if (message == null) return false;

        var upperMessage = message.toUpperCase();

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