package sqlancer.derby.schema;

import sqlancer.Randomly;
import sqlancer.derby.DerbyConnection;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.TableIndex;
import sqlancer.derby.DerbyGlobalState;


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

        String query = String.format(
                "SELECT columnname, columndatatype FROM sys.syscolumns c, sys.systables t " +
                        "WHERE c.referenceid = t.tableid AND t.tablename = '%s'", tableName);

        List<List<Object>> columnResults = connection.executeAndGet(query);

        for (List<Object> row : columnResults) {
            String columnName = (String) row.get(0);
            String dataType = (String) row.get(1);
            columns.add(new DerbyColumn(columnName, dataType));
        }

        return columns;
    }

    private static List<TableIndex> getTableIndexes(DerbyConnection connection, String tableName)
            throws SQLException {
        try {
            String query = String.format(
                    "SELECT conglomeratename, isindex FROM sys.sysconglomerates WHERE tableid = " +
                            "(SELECT tableid FROM sys.systables WHERE tablename = '%s')", tableName);

            List<List<Object>> indexResults = connection.executeAndGet(query);
            List<TableIndex> indexes = new ArrayList<>();

            for (List<Object> row : indexResults) {
                String indexName = (String) row.get(0);
                boolean isIndex = Boolean.TRUE.equals(row.get(1));
                if (isIndex) {
                    indexes.add(new TableIndex(indexName));
                }
            }

            return indexes;
        } catch (SQLException e) {
            return new ArrayList<>();
        }
    }

    @Override
    public DerbyTable getRandomTable() {
        return Randomly.fromList(getDatabaseTables());
    }
}