package sqlancer.derby.schema;

import sqlancer.common.schema.AbstractTableColumn;

public class DerbyColumn extends AbstractTableColumn<DerbyTable, DerbyDataType> {

    private final String dataType;

    public DerbyColumn(String name, DerbyTable table, String dataType) {
        super(name, table, DerbyDataType.fromString(dataType));
        this.dataType = dataType;
    }

    public String getDataType() {
        return dataType;
    }

    @Override
    public DerbyDataType getType() {
        return super.getType();
    }

    public String getFullType() {
        return dataType;
    }
}