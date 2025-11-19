package sqlancer.derby;

import sqlancer.*;
import sqlancer.common.query.Query;
import sqlancer.derby.schema.DerbySchema;
import sqlancer.derby.log.DerbyLoggableFactory;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Derby 全局状态管理
 * 负责数据库连接、Schema 读取和查询执行管理
 */

//protected C databaseConnection;
//private Randomly r;
//private MainOptions options;
//private O dbmsSpecificOptions;
//private S schema;
//private Main.StateLogger logger;
//private StateToReproduce state;
//private Main.QueryManager<C> manager;
//private String databaseName;


public class DerbyGlobalState extends GlobalState<DerbyOptions, DerbySchema, DerbyConnection> {


    DerbyLoggableFactory loggerNew = new DerbyLoggableFactory();

    protected void initializeConnection() throws SQLException {
        String url = String.format("jdbc:derby:%s;create=true",
                getDbmsSpecificOptions().getDbPath());
        DerbyConnection connection = new DerbyConnection(DriverManager.getConnection(url));
        System.out.println(connection.getDatabaseVersion());
        setConnection(connection);// 根据DerbyConnection类 创建链接
        System.out.println("Connected to Derby"); // debugging
    }

    @Override
    protected DerbySchema readSchema() throws Exception {
        // 从数据库连接读取 Schema 信息
        return DerbySchema.fromConnection(getConnection());
    }

    @Override
    protected void executeEpilogue(Query<?> q, boolean success, ExecutionTimer timer) throws Exception {
        // 查询执行后的处理逻辑
        if (timer != null && getOptions().logExecutionTime()) {
            timer.end();
            loggerNew.logTime(timer.asString());
        }
        if (!success) {
            // 记录失败的查询
            loggerNew.logCurrentState("Query failed: " + q.getLogString());
        }
    }


    protected void generateDatabase() throws Exception {
        // 生成测试数据库（表结构和初始数据）
        //new DerbyDatabaseGenerator(this).generateDatabase();
    }

    public DerbyOptions getDerbyOptions() {
        return getDbmsSpecificOptions();
    }
}