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
import java.util.ArrayList;
import java.util.List;


//protected C databaseConnection;
//private Randomly r;
//private MainOptions options;
//private O dbmsSpecificOptions;
//private S schema;
//private Main.StateLogger logger;
//private StateToReproduce state;
//private Main.QueryManager<C> manager;
//private String databaseName;
/**
 * Derby 全局状态管理
 * 负责数据库连接、Schema 读取和查询执行管理
 */
public class DerbyGlobalState extends GlobalState<DerbyOptions, DerbySchema, DerbyConnection> {


    DerbyLoggableFactory loggerNew = new DerbyLoggableFactory();

    protected void initializeConnection() throws SQLException {
        String url = String.format("jdbc:derby:%s;create=true",
                getDbmsSpecificOptions().getDbPath());
        DerbyConnection connection = new DerbyConnection(DriverManager.getConnection(url));
        System.out.println(connection.getDatabaseVersion());
        setConnection(connection);
        if (connection.isValid()) {
            System.out.println("Connected to Derby");
        }
        else {
            System.out.println("INVALID CONNECTION");
        }
        // debugging
    }

    @Override
    protected DerbySchema readSchema() throws Exception {
        // 从数据库连接读取 Schema 信息
        return DerbySchema.fromConnection(getConnection());
    }

    @Override
    protected void executeEpilogue(Query<?> q, boolean success, ExecutionTimer timer) throws Exception {
        if (timer != null && getOptions().logExecutionTime()) {
            timer.end();
            loggerNew.logTime(timer.asString());
        }
        if (!success) {
            loggerNew.logCurrentState("Query failed: " + q.getLogString());
        }
    }


    protected void generateDatabase() throws Exception {
        // 生成测试数据库（表结构和初始数据）
        new DerbyDatabaseGenerator(this).generateDatabase();
    }

    public DerbyOptions getDerbyOptions() {
        return getDbmsSpecificOptions();
    }

    public void executeStatement(SQLQueryAdapter query) throws SQLException {
        // 直接调用 DerbyConnection 中的 executeStatement 方法、
        String sql = query.getQueryString();
        String cleanedSql = removeTrailingSemicolon(sql.trim());
        getConnection().executeStatement(cleanedSql);
    }


    public List<List<Object>> executeStatementAndGet(SQLQueryAdapter query) throws SQLException {
        // 直接调用 DerbyConnection 中的 executeAndGet 方法
        String sql = query.getQueryString();
        String cleanedSql = removeTrailingSemicolon(sql.trim());
        return getConnection().executeAndGet(cleanedSql);
    }


    public SQLancerResultSet executeStatementAndGetAsResultSet(SQLQueryAdapter query) throws SQLException {
        // 使用 DerbyConnection 的 executeStatementAndGet 方法
        String sql = query.getQueryString();
        String cleanedSql = removeTrailingSemicolon(sql.trim());
        return getConnection().executeStatementAndGet(cleanedSql);
    }

    public DerbyConnection getConnection() {
        return (DerbyConnection) super.getConnection();
    }

    public void executeSQL(String sql) throws SQLException {
        String cleanedSql = removeTrailingSemicolon(sql.trim());
        getConnection().executeStatement(cleanedSql);
    }

    public List<List<Object>> executeQuery(String sql) throws SQLException {
        // 移除 SQL 语句末尾的分号
        String cleanedSql = removeTrailingSemicolon(sql.trim());
        return getConnection().executeAndGet(cleanedSql);
    }

    public DerbyLoggableFactory getLoggerNew() {
        return loggerNew;
    }

    private String removeTrailingSemicolon(String sql) {
        // 如果 SQL 以分号结尾，移除它
        if (sql.endsWith(";")) {
            return sql.substring(0, sql.length() - 1);
        }
        return sql;
    }

    // ========== FIX: 添加强制刷新schema的方法，解决表创建后schema缓存未更新的问题 ==========
    public void refreshSchema() throws Exception {
        DerbySchema newSchema = readSchema();
        setSchema(newSchema);
        int tableCount = newSchema != null ? newSchema.getDatabaseTables().size() : 0;
        getLoggerNew().logInfo("Schema refreshed, now has " + tableCount + " tables");
    }

    public int getTableCount() {
        if (getSchema() == null) {
            return 0;
        }
        return getSchema().getDatabaseTables().size();
    }

}