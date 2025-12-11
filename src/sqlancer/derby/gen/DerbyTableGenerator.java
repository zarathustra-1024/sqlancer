package sqlancer.derby.gen;

import sqlancer.Randomly;
import sqlancer.derby.DerbyGlobalState;

import java.util.ArrayList;
import java.util.List;

public class DerbyTableGenerator {

//    public static String generateCreateTable(DerbyGlobalState globalState) {
//        // ========== 关键修改：使用 getDatabaseTables() 而不是 getTables() ==========
//        int tableCount = globalState.getSchema() != null ?
//                globalState.getSchema().getDatabaseTables().size() : 0;
//        String tableName = "t" + tableCount;
//        return generateCreateTableWithName(globalState, tableName);
//    }

    // ========== 新增：指定表名的创建方法 ==========
    public static String generateCreateTableWithName(DerbyGlobalState globalState, String tableName) {
        List<String> columns = new ArrayList<>();
        int columnCount = 2 + globalState.getRandomly().getInteger(0, 3);

        columns.add("id INTEGER NOT NULL");

        for (int i = 1; i < columnCount; i++) {
            String columnName = "col" + i;
            String type = getRandomType();
            String definition = columnName + " " + type;

            if (Randomly.getBoolean() && !type.contains("TIMESTAMP")) {
                definition += " NOT NULL";
            }

            columns.add(definition);
        }

        return String.format("CREATE TABLE %s (%s)", tableName, String.join(", ", columns));
    }

    private static String getRandomType() {
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
        return Randomly.fromOptions(types);
    }
}
