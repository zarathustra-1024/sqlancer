package sqlancer.derby.schema;

import sqlancer.common.schema.AbstractTableColumn;

public class DerbyColumn extends AbstractTableColumn<DerbyTable, DerbyDataType> {

    private final String dataType;

    public DerbyColumn(String name, String dataType) {
        super(name, null, null);
        this.dataType = dataType;
    }

    public String getDataType() {
        return dataType;
    }

    @Override
    public DerbyDataType getType() {
        return DerbyDataType.fromString(dataType);
    }
}
