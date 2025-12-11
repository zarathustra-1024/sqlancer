package sqlancer.derby.gen;

import sqlancer.derby.DerbyGlobalState;
import sqlancer.common.query.SQLQueryAdapter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DerbyDatabaseGenerator {

    private final DerbyGlobalState globalState;

    public DerbyDatabaseGenerator(DerbyGlobalState globalState) {
        this.globalState = globalState;
    }

    public void generateDatabase() throws Exception {
        // ========== FIX: 使用带随机后缀的表名，防止表重复创建 ==========
        List<String> tableNames = new ArrayList<>();

        // 步骤1：创建表
        int tableCount = 2 + globalState.getRandomly().getInteger(0, 2);
        globalState.getLoggerNew().logInfo("Creating " + tableCount + " tables");

        // 生成一个基础随机数，确保本次测试的所有表名有共同前缀
        long timestamp = System.currentTimeMillis();
        int testId = globalState.getRandomly().getInteger(0, 10000);

        for (int i = 0; i < tableCount; i++) {
            // 使用时间戳+测试ID+表序号作为表名，确保唯一性
            String tableName = String.format("t%d_%d_%d", i, timestamp, testId);

            String createTable = generateCreateTableWithName(globalState, tableName);
            globalState.getLoggerNew().logQuery("Create table: " + createTable);

            SQLQueryAdapter query = new SQLQueryAdapter(createTable);
            globalState.executeStatement(query);
            tableNames.add(tableName);
        }

        // ========== MOD: 强制更新schema缓存，确保表创建后能立即被识别 ==========
        try {
            globalState.refreshSchema();
        } catch (Exception e) {
            Thread.sleep(100);  // 等待后重试
            globalState.refreshSchema();
        }

        globalState.getLoggerNew().logInfo("Tables created: " + tableNames);

        // 步骤2：插入数据
        for (int i = 0; i < tableNames.size(); i++) {
            int insertCount = 1 + globalState.getRandomly().getInteger(0, 2);
            globalState.getLoggerNew().logInfo("Inserting " + insertCount + " rows into table " + tableNames.get(i));

            for (int j = 0; j < insertCount; j++) {
                // MOD: 使用 getDatabaseTables() 而不是 getTables() 检查schema状态
                if (globalState.getSchema() == null || globalState.getSchema().getDatabaseTables().isEmpty()) {
                    globalState.getLoggerNew().logInfo("Schema is empty, refreshing...");
                    globalState.refreshSchema();
                }

                // FIX: 使用实际的表名而不是重新生成，确保插入到正确的表
                String insert = generateInsertForTable(globalState, tableNames.get(i));
                globalState.getLoggerNew().logQuery("Insert: " + insert);

                SQLQueryAdapter insertQuery = new SQLQueryAdapter(insert);
                globalState.executeStatement(insertQuery);
            }
        }

        // MOD: 使用 getDatabaseTables() 获取实际表数量
        int actualTableCount = globalState.getSchema() != null ?
                globalState.getSchema().getDatabaseTables().size() : 0;
        globalState.getLoggerNew().logInfo("Database generation completed: " +
                actualTableCount + " tables created");
    }


    // ========== 新增：指定表名的创建方法 ==========
    private String generateCreateTableWithName(DerbyGlobalState globalState, String tableName) {
        // 修改：调用 DerbyTableGenerator 的静态方法
        return DerbyTableGenerator.generateCreateTableWithName(globalState, tableName);
    }

    // ========== 新增：指定表名的插入方法 ==========
    private String generateInsertForTable(DerbyGlobalState globalState, String tableName) {
        // 简化插入生成逻辑
        DerbyExpressionGenerator gen = new DerbyExpressionGenerator(globalState);

        // 生成一些随机列和值
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();

        int columnCount = 2 + globalState.getRandomly().getInteger(0, 2);
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                columns.append(", ");
                values.append(", ");
            }
            columns.append("c").append(i);
            values.append(gen.generateConstant());
        }

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableName, columns.toString(), values.toString());
    }
}

