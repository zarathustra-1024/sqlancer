package sqlancer.derby;

import sqlancer.*;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.derby.gen.DerbyDatabaseGenerator;
import sqlancer.derby.schema.DerbySchema;
import sqlancer.derby.log.DerbyLoggableFactory;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class DerbyGlobalState extends GlobalState<DerbyOptions, DerbySchema, DerbyConnection> {

    // 保持原有的 loggerNew
    DerbyLoggableFactory loggerNew = new DerbyLoggableFactory();

    protected void initializeConnection() throws SQLException {
        var url = String.format("jdbc:derby:%s;create=true",
                getDbmsSpecificOptions().getDbPath());

        var connection = new DerbyConnection(DriverManager.getConnection(url));
        setConnection(connection);

        // 新增：初始化错误日志文件
        var errorLogFile = getDbmsSpecificOptions().getErrorLogFile();
        if (errorLogFile != null && !errorLogFile.trim().isEmpty()) {
            loggerNew.initErrorLogFile(errorLogFile);
        }

        if (connection.isValid()) {
            loggerNew.logInfo("Connected to " + connection.getDatabaseVersion());
        } else {
            loggerNew.logInfo("INVALID CONNECTION");
        }
    }

    @Override
    protected DerbySchema readSchema() throws Exception {
        return DerbySchema.fromConnection(getConnection());
    }

    @Override
    protected void executeEpilogue(Query<?> q, boolean success, ExecutionTimer timer) throws Exception {
        if (timer != null && getOptions().logExecutionTime()) {
            timer.end();
            loggerNew.logTime(timer.asString());
        }

        if (!success) {
            // 修改：使用 logError 使错误被记录到文件
            loggerNew.logError("Query failed: " + q.getLogString());
        }
    }

    protected void generateDatabase() throws Exception {
        new DerbyDatabaseGenerator(this).generateDatabase();
    }

    public DerbyOptions getDerbyOptions() {
        return getDbmsSpecificOptions();
    }

    public void executeStatement(SQLQueryAdapter query) throws SQLException {
        var sql = query.getQueryString();
        var cleanedSql = removeTrailingSemicolon(sql.trim());
        getConnection().executeStatement(cleanedSql);
    }

    public List<List<Object>> executeStatementAndGet(SQLQueryAdapter query) throws SQLException {
        var sql = query.getQueryString();
        var cleanedSql = removeTrailingSemicolon(sql.trim());
        return getConnection().executeAndGet(cleanedSql);
    }

    public SQLancerResultSet executeStatementAndGetAsResultSet(SQLQueryAdapter query) throws SQLException {
        var sql = query.getQueryString();
        var cleanedSql = removeTrailingSemicolon(sql.trim());
        return getConnection().executeStatementAndGet(cleanedSql);
    }

    public DerbyConnection getConnection() {
        return (DerbyConnection) super.getConnection();
    }

    public void executeSQL(String sql) throws SQLException {
        var cleanedSql = removeTrailingSemicolon(sql.trim());
        getConnection().executeStatement(cleanedSql);
    }

    public List<List<Object>> executeQuery(String sql) throws SQLException {
        var cleanedSql = removeTrailingSemicolon(sql.trim());
        return getConnection().executeAndGet(cleanedSql);
    }

    public DerbyLoggableFactory getLoggerNew() {
        return loggerNew;
    }

    private String removeTrailingSemicolon(String sql) {
        if (sql.endsWith(";")) {
            return sql.substring(0, sql.length() - 1);
        }
        return sql;
    }

    public void refreshSchema() throws Exception {
        var newSchema = readSchema();
        setSchema(newSchema);
        var tableCount = newSchema != null ? newSchema.getDatabaseTables().size() : 0;
        loggerNew.logInfo("Schema refreshed, now has " + tableCount + " tables");
    }

    public int getTableCount() {
        if (getSchema() == null) {
            return 0;
        }
        return getSchema().getDatabaseTables().size();
    }
}