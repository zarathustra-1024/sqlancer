package sqlancer.derby.gen;

import sqlancer.Randomly;
import sqlancer.derby.DerbyGlobalState;
import sqlancer.derby.schema.DerbyColumn;
import sqlancer.derby.schema.DerbyTable;

import java.util.ArrayList;
import java.util.List;

public class DerbyInsertGenerator {

    public static String generateInsert(DerbyGlobalState globalState) {
        // MOD: 使用 getDatabaseTables() 而不是 getTables() 检查schema状态
        if (globalState.getSchema() == null || globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IllegalStateException("Cannot generate INSERT: schema is empty or not initialized");
        }

        DerbyTable table = globalState.getSchema().getRandomTable();
        List<DerbyColumn> columns = table.getColumns();

        // ========== FIX: 添加列检查，防止空列列表导致的问题 ==========
        if (columns.isEmpty()) {
            return generateSimpleInsert(globalState, table.getName());
        }

        List<String> columnNames = new ArrayList<>();
        List<String> values = new ArrayList<>();

        DerbyExpressionGenerator gen = new DerbyExpressionGenerator(globalState);

        for (DerbyColumn column : columns) {
            columnNames.add(column.getName());
            values.add(gen.generateConstant());
        }

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                table.getName(),
                String.join(", ", columnNames),
                String.join(", ", values));
    }

    // ========== ADD: 新增简单插入生成方法，用于处理空列的情况 ==========
    private static String generateSimpleInsert(DerbyGlobalState globalState, String tableName) {
        DerbyExpressionGenerator gen = new DerbyExpressionGenerator(globalState);

        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();

        int columnCount = 2;
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                columns.append(", ");
                values.append(", ");
            }
            columns.append("col").append(i);
            values.append(gen.generateConstant());
        }

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableName, columns.toString(), values.toString());
    }
}