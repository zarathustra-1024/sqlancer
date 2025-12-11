package sqlancer.derby.schema;

import sqlancer.Randomly;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.TableIndex;
import sqlancer.derby.DerbyGlobalState;

import java.util.List;

public class DerbyTable extends AbstractTable<DerbyColumn, TableIndex, DerbyGlobalState> {

    public DerbyTable(String name, List<DerbyColumn> columns, List<TableIndex> indexes) {
        super(name, columns, indexes, false);
    }

    public DerbyColumn getRandomColumn() {
        List<DerbyColumn> columns = getColumns();
        if (columns.isEmpty()) {
            return null;
        }
        return Randomly.fromList(columns);
    }

    @Override
    public long getNrRows(DerbyGlobalState globalState) {
        return 100;
    }
}