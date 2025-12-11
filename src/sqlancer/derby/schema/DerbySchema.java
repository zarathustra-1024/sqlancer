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

            // 创建表对象
            DerbyTable table = new DerbyTable(tableName, columns, indexes);
            tables.add(table);
        }

        return new DerbySchema(tables);
    }

    private static List<DerbyColumn> getTableColumns(DerbyConnection connection, String tableName)
            throws SQLException {
        List<DerbyColumn> columns = new ArrayList<>();

        // 方法1：尝试使用 DatabaseMetaData（推荐）
        try {
            DatabaseMetaData metaData = connection.getConnection().getMetaData();
            try (ResultSet rs = metaData.getColumns(null, "APP", tableName, "%")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("TYPE_NAME");
                    int columnSize = rs.getInt("COLUMN_SIZE");

                    // 处理带大小的类型
                    if (columnSize > 0 && (dataType.equals("VARCHAR") || dataType.equals("CHAR"))) {
                        dataType = dataType + "(" + columnSize + ")";
                    } else if (dataType.equals("DECIMAL") || dataType.equals("NUMERIC")) {
                        int decimalDigits = rs.getInt("DECIMAL_DIGITS");
                        dataType = dataType + "(" + columnSize + "," + decimalDigits + ")";
                    }

                    columns.add(new DerbyColumn(columnName, null, dataType));
                }
            }
            return columns;
        } catch (Exception e) {
            System.err.println("Warning: DatabaseMetaData failed, using fallback method: " + e.getMessage());
        }

        // 方法2：使用简化的查询，避免 CASE 表达式中的 LIKE
        try {
            // 使用 SUBSTRING 和位置检查来代替 LIKE
            String query =
                    "SELECT columnname, " +
                            "CASE " +
                            "  WHEN SUBSTR(columndatatype, 1, 7) = 'INTEGER' THEN 'INTEGER' " +
                            "  WHEN SUBSTR(columndatatype, 1, 7) = 'VARCHAR' THEN 'VARCHAR' " +
                            "  WHEN SUBSTR(columndatatype, 1, 4) = 'CHAR' THEN 'CHAR' " +
                            "  WHEN SUBSTR(columndatatype, 1, 6) = 'DOUBLE' THEN 'DOUBLE' " +
                            "  WHEN SUBSTR(columndatatype, 1, 4) = 'DATE' THEN 'DATE' " +
                            "  WHEN SUBSTR(columndatatype, 1, 9) = 'TIMESTAMP' THEN 'TIMESTAMP' " +
                            "  WHEN SUBSTR(columndatatype, 1, 7) = 'BOOLEAN' THEN 'BOOLEAN' " +
                            "  WHEN SUBSTR(columndatatype, 1, 8) = 'SMALLINT' THEN 'SMALLINT' " +
                            "  WHEN SUBSTR(columndatatype, 1, 6) = 'BIGINT' THEN 'BIGINT' " +
                            "  WHEN SUBSTR(columndatatype, 1, 4) = 'REAL' THEN 'REAL' " +
                            "  WHEN SUBSTR(columndatatype, 1, 7) = 'DECIMAL' THEN 'DECIMAL' " +
                            "  WHEN SUBSTR(columndatatype, 1, 5) = 'FLOAT' THEN 'FLOAT' " +
                            "  WHEN SUBSTR(columndatatype, 1, 4) = 'TIME' THEN 'TIME' " +
                            "  WHEN SUBSTR(columndatatype, 1, 4) = 'BLOB' THEN 'BLOB' " +
                            "  WHEN SUBSTR(columndatatype, 1, 4) = 'CLOB' THEN 'CLOB' " +
                            "  ELSE 'VARCHAR' " +
                            "END as datatype " +
                            "FROM sys.syscolumns c, sys.systables t " +
                            "WHERE c.referenceid = t.tableid AND t.tablename = '" + tableName + "'";

            List<List<Object>> columnResults = connection.executeAndGet(query);

            for (List<Object> row : columnResults) {
                String columnName = (String) row.get(0);
                String dataType = (String) row.get(1);
                columns.add(new DerbyColumn(columnName, null, dataType));
            }

            return columns;
        } catch (Exception e) {
            System.err.println("Warning: Fallback method also failed: " + e.getMessage());

            // 方法3：使用最简单的查询，不进行类型映射
            try {
                String query = "SELECT columnname, 'VARCHAR' as datatype " +
                        "FROM sys.syscolumns c, sys.systables t " +
                        "WHERE c.referenceid = t.tableid AND t.tablename = '" + tableName + "'";

                List<List<Object>> columnResults = connection.executeAndGet(query);

                for (List<Object> row : columnResults) {
                    String columnName = (String) row.get(0);
                    columns.add(new DerbyColumn(columnName, null, "VARCHAR"));
                }
            } catch (Exception e2) {
                System.err.println("Error: All methods failed to get column information: " + e2.getMessage());
                // 返回空列表，让表至少能被创建
            }

            return columns;
        }
    }

    private static List<TableIndex> getTableIndexes(DerbyConnection connection, String tableName)
            throws SQLException {
        try {
            String query = "SELECT conglomeratename FROM sys.sysconglomerates WHERE tableid = " +
                    "(SELECT tableid FROM sys.systables WHERE tablename = '" + tableName + "') AND isindex = true";

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

    public List<DerbyTable> getDatabaseTables() {
        return getTables(t -> true);
    }

    @Override
    public DerbyTable getRandomTable() {
        List<DerbyTable> tables = getDatabaseTables();
        if (tables.isEmpty()) {
            throw new IllegalStateException("No tables available in schema");
        }
        return Randomly.fromList(tables);
    }
}