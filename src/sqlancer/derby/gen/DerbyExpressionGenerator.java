package sqlancer.derby.gen;

import sqlancer.Randomly;
import sqlancer.derby.DerbyGlobalState;
import sqlancer.derby.schema.DerbyColumn;
import sqlancer.derby.schema.DerbyTable;

public class DerbyExpressionGenerator {

    private final DerbyGlobalState globalState;

    public DerbyExpressionGenerator(DerbyGlobalState globalState) {
        this.globalState = globalState;
    }

    public String generateConstant() {
        if (Randomly.getBoolean()) {
            return "'" + globalState.getRandomly().getString() + "'";
        } else {
            return String.valueOf(globalState.getRandomly().getInteger());
        }
    }

    public String generateColumnReference() {
        DerbyTable table = globalState.getSchema().getRandomTable();
        DerbyColumn column = table.getRandomColumn();
        return column.getName();
    }

    public String generatePredicate() {
        return generateColumnReference() + " = " + generateConstant();
    }
}