package sqlancer.derby.schema;

import sqlancer.Randomly;

public enum DerbyDataType {
    INTEGER,
    VARCHAR,
    DATE,
    TIMESTAMP,
    DOUBLE,
    CHAR,
    BOOLEAN,
    SMALLINT,
    BIGINT,
    REAL,
    DECIMAL,
    NUMERIC,
    FLOAT,
    TIME,
    BLOB,
    CLOB;

    public static DerbyDataType getRandom() {
        return Randomly.fromOptions(values());
    }

    public static DerbyDataType fromString(String typeName) {
        if (typeName == null || typeName.isEmpty()) {
            return VARCHAR;
        }

        String upperType = typeName.toUpperCase();
        if (upperType.contains("(")) {
            upperType = upperType.substring(0, upperType.indexOf("("));
        }

        for (DerbyDataType type : DerbyDataType.values()) {
            if (upperType.contains(type.name())) {
                return type;
            }
        }

        if (upperType.contains("INT") && !upperType.contains("BIGINT") && !upperType.contains("SMALLINT")) {
            return INTEGER;
        } else if (upperType.contains("NUMERIC") || upperType.contains("DEC")) {
            return DECIMAL;
        } else if (upperType.contains("FLOAT")) {
            return FLOAT;
        } else if (upperType.contains("BOOL")) {
            return BOOLEAN;
        } else if (upperType.contains("STRING") || upperType.contains("TEXT")) {
            return VARCHAR;
        } else if (upperType.contains("CHARACTER")) {
            return CHAR;
        }

        return VARCHAR;
    }
}