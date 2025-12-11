package sqlancer.derby.schema;

import sqlancer.Randomly;
import sqlancer.derby.DerbyConnection;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.TableIndex;
import sqlancer.derby.DerbyGlobalState;


import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DerbySchema extends AbstractSchema<DerbyGlobalState, DerbyTable> {

    public DerbySchema(List<DerbyTable> tables) {
        super(tables);
    }

    public static DerbySchema fromConnection(DerbyConnection connection) throws SQLException {
        List<DerbyTable> tables = new ArrayList<>();

        String query = "SELECT tablename FROM sys.systables WHERE tabletype = 'T' AND tablename NOT LIKE 'SYS%'";
        List<List<Object>> tableResults = connection.executeAndGet(query);

        for (List<Object> row : tableResults) {
            String tableName = (String) row.get(0);
            List<DerbyColumn> columns = getTableColumns(connection, tableName);
            List<TableIndex> indexes = getTableIndexes(connection, tableName);

            DerbyTable table = new DerbyTable(tableName, columns, indexes);
            tables.add(table);
        }

        return new DerbySchema(tables);
    }

    private static List<DerbyColumn> getTableColumns(DerbyConnection connection, String tableName)
            throws SQLException {
        List<DerbyColumn> columns = new ArrayList<>();

        try {
            DatabaseMetaData metaData = connection.getConnection().getMetaData();
            // 使用 DatabaseMetaData 获取列信息，避免 TypeDescriptorImpl 转换问题
            try (ResultSet rs = metaData.getColumns(null, "APP", tableName, "%")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("TYPE_NAME");
                    int columnSize = rs.getInt("COLUMN_SIZE");

                    // 如果类型包含大小信息，添加到类型字符串中
                    if (columnSize > 0 && (dataType.equals("VARCHAR") || dataType.equals("CHAR"))) {
                        dataType = dataType + "(" + columnSize + ")";
                    }

                    columns.add(new DerbyColumn(columnName, dataType));
                }
            }
        } catch (SQLException e) {
            // 如果 DatabaseMetaData 失败，回退到原始查询，但需要处理 TypeDescriptorImpl
            System.err.println("Warning: Using fallback method for column metadata: " + e.getMessage());

            // 原始查询（需要修复）
            String query = String.format(
                    "SELECT columnname, CAST(columndatatype AS VARCHAR(1000)) FROM sys.syscolumns c, sys.systables t " +
                            "WHERE c.referenceid = t.tableid AND t.tablename = '%s'", tableName);

            List<List<Object>> columnResults = connection.executeAndGet(query);

            for (List<Object> row : columnResults) {
                String columnName = (String) row.get(0);
                String dataType = (String) row.get(1);
                columns.add(new DerbyColumn(columnName, dataType));
            }
        }

        return columns;
    }

    private static List<TableIndex> getTableIndexes(DerbyConnection connection, String tableName)
            throws SQLException {
        try {
            // 使用 Derby 的系统表查询索引
            String query = String.format(
                    "SELECT i.indexname FROM sys.sysindexes i, sys.systables t " +
                            "WHERE i.tableid = t.tableid AND t.tablename = '%s'", tableName);

            List<List<Object>> indexResults = connection.executeAndGet(query);
            List<TableIndex> indexes = new ArrayList<>();

            for (List<Object> row : indexResults) {
                String indexName = (String) row.get(0);
                indexes.add(new TableIndex(indexName));
            }

            return indexes;
        } catch (SQLException e) {
            return new ArrayList<>();
        }
    }

    // ========== FIX: 添加 getDatabaseTables() 方法适配 AbstractSchema 接口 ==========
    public List<DerbyTable> getDatabaseTables() {
        // 返回所有表（不过滤任何表），使用 getTables(t -> true) 实现
        return getTables(t -> true);
    }

    // ========== MOD: 修正 getRandomTable() 方法，使用 getDatabaseTables() 而不是直接调用 getTables() ==========
    @Override
    public DerbyTable getRandomTable() {
        List<DerbyTable> tables = getDatabaseTables();
        if (tables.isEmpty()) {
            throw new IllegalStateException("No tables available in schema");
        }
        return Randomly.fromList(tables);
    }
}