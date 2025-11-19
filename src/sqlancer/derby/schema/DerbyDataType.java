package sqlancer.derby.schema;

import sqlancer.Randomly;

/**
 * Derby 数据类型枚举
 * 定义支持的数据库类型和随机选择逻辑
 */
public enum DerbyDataType {
    INTEGER, VARCHAR, DATE, TIMESTAMP;

    public static DerbyDataType getRandom() {
        return Randomly.fromOptions(values());
    }

    public static DerbyDataType fromString(String typeName) {
        for (DerbyDataType type : DerbyDataType.values()) {
            if (typeName.toUpperCase().contains(type.name())) {
                return type;
            }
        }
        return VARCHAR; // 默认返回 VARCHAR
    }
}