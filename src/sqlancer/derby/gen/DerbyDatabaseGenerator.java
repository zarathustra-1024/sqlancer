package sqlancer.derby.gen;

import sqlancer.derby.DerbyGlobalState;
import sqlancer.derby.schema.DerbyTable;
import sqlancer.derby.schema.DerbyColumn;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.TableIndex;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DerbyDatabaseGenerator {

    private final DerbyGlobalState globalState;

    public DerbyDatabaseGenerator(DerbyGlobalState globalState) {
        this.globalState = globalState;
    }

    public void generateDatabase() throws Exception {
        List<TableInfo> tableInfos = new ArrayList<>();

        int tableCount = 2 + globalState.getRandomly().getInteger(0, 2);
        globalState.getLoggerNew().logInfo("Creating " + tableCount + " tables");

        // 步骤1：创建所有表
        for (int i = 0; i < tableCount; i++) {
            long timestamp = System.currentTimeMillis();
            int testId = globalState.getRandomly().getInteger(0, 10000);
            String tableName = String.format("t%d_%d_%d", i, timestamp, testId);

            String createTable = createSimpleTable(tableName);
            globalState.getLoggerNew().logQuery("Create table: " + createTable);

            SQLQueryAdapter query = new SQLQueryAdapter(createTable);
            globalState.executeStatement(query);

            // 解析CREATE TABLE语句获取列信息
            DerbyTable table = parseTableFromCreateStatement(tableName, createTable);
            tableInfos.add(new TableInfo(tableName, table));
        }

        // 步骤2：刷新schema以获取准确的表结构
        try {
            globalState.refreshSchema();
        } catch (Exception e) {
            Thread.sleep(100);
            globalState.refreshSchema();
        }

        // 步骤3：为每个表插入数据
        for (TableInfo tableInfo : tableInfos) {
            // 从schema中获取实际的表（包含正确的列信息）
            DerbyTable table = findTableByName(tableInfo.tableName);
            if (table == null) {
                table = tableInfo.table; // 如果找不到，使用我们解析的
            }

            int insertCount = 1 + globalState.getRandomly().getInteger(0, 2);
            globalState.getLoggerNew().logInfo("Inserting " + insertCount + " rows into table " + table.getName());

            for (int j = 0; j < insertCount; j++) {
                // 使用实际的表结构生成插入语句
                String insert = generateInsertForTable(globalState, table);
                globalState.getLoggerNew().logQuery("Insert: " + insert);

                try {
                    SQLQueryAdapter insertQuery = new SQLQueryAdapter(insert);
                    globalState.executeStatement(insertQuery);
                } catch (SQLException e) {
                    // 如果插入失败，尝试更简单的方法
                    globalState.getLoggerNew().logException(e);
                    try {
                        String simpleInsert = generateSimpleInsert(table.getName());
                        SQLQueryAdapter simpleQuery = new SQLQueryAdapter(simpleInsert);
                        globalState.executeStatement(simpleQuery);
                    } catch (SQLException e2) {
                        globalState.getLoggerNew().logInfo("Failed to insert into table " + table.getName());
                    }
                }
            }
        }

        globalState.getLoggerNew().logInfo("Database generation completed: " + tableInfos.size() + " tables created");
    }

    // ========== 简化表创建逻辑，使用扩展的类型系统 ==========
    private String createSimpleTable(String tableName) {
        List<String> columns = new ArrayList<>();

        // 总是创建一个ID列
        columns.add("id INTEGER NOT NULL");

        // 随机添加1-3个其他列
        int extraColumns = 1 + globalState.getRandomly().getInteger(0, 2);
        for (int i = 1; i <= extraColumns; i++) {
            String columnName = "col" + i;
            String type = getRandomType();
            columns.add(columnName + " " + type);
        }

        return String.format("CREATE TABLE %s (%s)", tableName, String.join(", ", columns));
    }

    // ========== 生成随机类型 ==========
    private String getRandomType() {
        // 使用扩展的类型
        String[] types = {
                "INTEGER",
                "VARCHAR(50)",
                "CHAR(20)",
                "DATE",
                "TIMESTAMP",
                "DOUBLE",
                "BOOLEAN",
                "SMALLINT",
                "BIGINT",
                "REAL",
                "DECIMAL(10,2)",
                "FLOAT",
                "TIME",
                "BLOB",
                "CLOB"
        };
        return types[globalState.getRandomly().getInteger(0, types.length - 1)];
    }

    // ========== 从CREATE TABLE语句解析表结构 ==========
    private DerbyTable parseTableFromCreateStatement(String tableName, String createTable) {
        List<DerbyColumn> columns = new ArrayList<>();
        List<TableIndex> indexes = new ArrayList<>();

        // 提取列定义部分
        int start = createTable.indexOf("(");
        int end = createTable.lastIndexOf(")");
        if (start != -1 && end != -1) {
            String columnDefs = createTable.substring(start + 1, end).trim();
            String[] parts = columnDefs.split(",");

            // 创建临时表对象（传入空列，稍后会设置）
            DerbyTable tempTable = new DerbyTable(tableName, new ArrayList<>(), new ArrayList<>());

            for (String part : parts) {
                part = part.trim();
                if (part.isEmpty()) continue;

                // 解析列名和类型
                String[] tokens = part.split("\\s+");
                if (tokens.length >= 2) {
                    String columnName = tokens[0];
                    StringBuilder typeBuilder = new StringBuilder();
                    for (int i = 1; i < tokens.length && !tokens[i].equalsIgnoreCase("NOT"); i++) {
                        if (typeBuilder.length() > 0) {
                            typeBuilder.append(" ");
                        }
                        typeBuilder.append(tokens[i]);
                    }
                    String dataType = typeBuilder.toString();

                    // 创建列，传入临时表对象
                    DerbyColumn column = new DerbyColumn(columnName, tempTable, dataType);
                    columns.add(column);
                }
            }
        }

        return new DerbyTable(tableName, columns, indexes);
    }

    // ========== 根据表名查找表 ==========
    private DerbyTable findTableByName(String tableName) {
        if (globalState.getSchema() == null) {
            return null;
        }

        for (DerbyTable table : globalState.getSchema().getDatabaseTables()) {
            if (table.getName().equalsIgnoreCase(tableName)) {
                return table;
            }
        }
        return null;
    }

    // ========== 为表生成插入语句 ==========
    private String generateInsertForTable(DerbyGlobalState globalState, DerbyTable table) {
        DerbyExpressionGenerator gen = new DerbyExpressionGenerator(globalState);
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();

        List<DerbyColumn> tableColumns = table.getColumns();

        if (tableColumns.isEmpty()) {
            return generateSimpleInsert(table.getName());
        }

        // 使用实际表中的列
        for (int i = 0; i < tableColumns.size(); i++) {
            if (i > 0) {
                columns.append(", ");
                values.append(", ");
            }

            DerbyColumn column = tableColumns.get(i);
            columns.append(column.getName());

            // 使用列的类型生成合适的值
            values.append(gen.generateValueForType(column.getType()));
        }

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                table.getName(), columns.toString(), values.toString());
    }

    // ========== 生成简化插入 ==========
    private String generateSimpleInsert(String tableName) {
        return String.format("INSERT INTO %s (id) VALUES (%d)",
                tableName, globalState.getRandomly().getInteger(1, 100));
    }

    // ========== 内部类：表信息 ==========
    private static class TableInfo {
        String tableName;
        DerbyTable table;

        TableInfo(String tableName, DerbyTable table) {
            this.tableName = tableName;
            this.table = table;
        }
    }
}