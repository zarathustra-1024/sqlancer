package sqlancer.derby.gen;

import sqlancer.Randomly;
import sqlancer.derby.DerbyGlobalState;
import sqlancer.derby.schema.DerbyColumn;
import sqlancer.derby.schema.DerbyDataType;
import sqlancer.derby.schema.DerbyTable;

import static sqlancer.derby.schema.DerbyDataType.*;

public class DerbyExpressionGenerator {

    private final DerbyGlobalState globalState;

    public DerbyExpressionGenerator(DerbyGlobalState globalState) {
        this.globalState = globalState;
    }

    public String generateConstant() {
        if (Randomly.getBoolean()) {
            return "'" + escapeString(globalState.getRandomly().getString()) + "'";
        } else {
            return String.valueOf(globalState.getRandomly().getInteger());
        }
    }

    public String generateValueForType(DerbyDataType dataType) {
        if (dataType == null) {
            return generateConstant();
        }

        switch (dataType) {
            case INTEGER:
                return String.valueOf(globalState.getRandomly().getInteger());

            case SMALLINT:
                return String.valueOf(globalState.getRandomly().getInteger(-32768, 32767));

            case BIGINT:
                // 修正：为 getLong 方法提供范围参数
                return String.valueOf(globalState.getRandomly().getLong(-1000000L, 1000000L));

            case DOUBLE:
            case FLOAT:
            case REAL:
                double value = globalState.getRandomly().getDouble();
                // 避免科学计数法
                return String.format("%.6f", value);

            case DECIMAL:
            case NUMERIC:
                double decimalValue = globalState.getRandomly().getDouble();
                return String.format("%.2f", decimalValue);

            case VARCHAR:
            case CHAR:
                String randomString = globalState.getRandomly().getString();
                // 限制字符串长度
                if (randomString.length() > 100) {
                    randomString = randomString.substring(0, 100);
                }
                return "'" + escapeString(randomString) + "'";

            case DATE:
                int year = 2000 + globalState.getRandomly().getInteger(0, 25);
                int month = 1 + globalState.getRandomly().getInteger(0, 11);
                int day = 1 + globalState.getRandomly().getInteger(0, 27);
                return String.format("DATE('%04d-%02d-%02d')", year, month, day);

            case TIMESTAMP:
                int hour = globalState.getRandomly().getInteger(0, 23);
                int minute = globalState.getRandomly().getInteger(0, 59);
                int second = globalState.getRandomly().getInteger(0, 59);
                return String.format("TIMESTAMP('2023-01-01 %02d:%02d:%02d')", hour, minute, second);

            case TIME:
                hour = globalState.getRandomly().getInteger(0, 23);
                minute = globalState.getRandomly().getInteger(0, 59);
                second = globalState.getRandomly().getInteger(0, 59);
                return String.format("TIME('%02d:%02d:%02d')", hour, minute, second);

            case BOOLEAN:
                return globalState.getRandomly().getBoolean() ? "TRUE" : "FALSE";

            case BLOB:
            case CLOB:
                // 对于LOB类型，使用简单的值
                return "NULL";

            default:
                return generateConstant();
        }
    }

    public String generateValueForType(String dataType) {
        return generateValueForType(DerbyDataType.fromString(dataType));
    }

    private String escapeString(String str) {
        return str.replace("'", "''");
    }

    public String generateColumnReference() {
        DerbyTable table = globalState.getSchema().getRandomTable();
        DerbyColumn column = table.getRandomColumn();
        return column.getName();
    }

    public String generatePredicate() {
        DerbyColumn column = globalState.getSchema().getRandomTable().getRandomColumn();
        DerbyDataType type = column.getType();

        if (type == DerbyDataType.VARCHAR || type == DerbyDataType.CHAR) {
            // 字符串类型的条件
            return column.getName() + " LIKE '%" + escapeString(globalState.getRandomly().getString()) + "%'";
        } else {
            // 数值类型的条件
            return column.getName() + " = " + generateValueForType(type);
        }
    }
}