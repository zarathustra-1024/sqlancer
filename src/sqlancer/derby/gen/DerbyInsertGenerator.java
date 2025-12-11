package sqlancer.derby.gen;

import sqlancer.Randomly;
import sqlancer.derby.DerbyGlobalState;
import sqlancer.derby.schema.DerbyColumn;
import sqlancer.derby.schema.DerbyTable;

import java.util.ArrayList;
import java.util.List;

public class DerbyInsertGenerator {

    public static String generateInsert(DerbyGlobalState globalState) {
        if (globalState.getSchema() == null || globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IllegalStateException("Cannot generate INSERT: schema is empty or not initialized");
        }

        DerbyTable table = globalState.getSchema().getRandomTable();
        List<DerbyColumn> columns = table.getColumns();

        if (columns.isEmpty()) {
            return generateSimpleInsert(globalState, table.getName());
        }

        DerbyExpressionGenerator gen = new DerbyExpressionGenerator(globalState);
        List<String> columnNames = new ArrayList<>();
        List<String> values = new ArrayList<>();

        for (DerbyColumn column : columns) {
            columnNames.add(column.getName());
            values.add(gen.generateValueForType(column.getType()));
        }

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                table.getName(),
                String.join(", ", columnNames),
                String.join(", ", values));
    }

    // ========== ADD: 新增简单插入生成方法，用于处理空列的情况 ==========
    private static String generateSimpleInsert(DerbyGlobalState globalState, String tableName) {
        return String.format("INSERT INTO %s (id) VALUES (%d)",
                tableName, globalState.getRandomly().getInteger(1, 100));
    }
}