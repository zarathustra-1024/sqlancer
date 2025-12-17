package sqlancer.derby;

import com.google.auto.service.AutoService;
import sqlancer.*;
import sqlancer.common.log.LoggableFactory;
import sqlancer.common.log.SQLLoggableFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.Query;
import sqlancer.derby.gen.DerbyInsertGenerator;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Derby 数据库适配器主入口点
 */
@AutoService(DatabaseProvider.class)
public class DerbyProvider extends ProviderAdapter<DerbyGlobalState, DerbyOptions, DerbyConnection> {

    public DerbyProvider() {
        super(DerbyGlobalState.class, DerbyOptions.class);
    }

    @Override
    protected void checkViewsAreValid(DerbyGlobalState globalState) throws SQLException {
        // Derby 不支持视图，空实现
    }

    @Override
    public void generateDatabase(DerbyGlobalState globalState) throws Exception {
        globalState.initializeConnection();
        globalState.generateDatabase();
    }

    @Override
    protected TestOracle<DerbyGlobalState> getTestOracle(DerbyGlobalState globalState) throws Exception {
        var testOracleFactory = globalState.getDbmsSpecificOptions().getTestOracleFactory();

        boolean testOracleRequiresMoreThanZeroRows = testOracleFactory.stream()
                .anyMatch(OracleFactory::requiresAllTablesToContainRows);
        boolean userRequiresMoreThanZeroRows = globalState.getOptions().testOnlyWithMoreThanZeroRows();
        boolean checkZeroRows = testOracleRequiresMoreThanZeroRows || userRequiresMoreThanZeroRows;

        if (checkZeroRows && globalState.getSchema().containsTableWithZeroRows(globalState)) {
            if (globalState.getOptions().enableQPG()) {
                addRowsToAllTables(globalState);
            } else {
                throw new IgnoreMeException();
            }
        }

        if (testOracleFactory.size() == 1) {
            return testOracleFactory.get(0).create(globalState);
        } else {
            return new CompositeTestOracle<>(testOracleFactory.stream().map(o -> {
                try {
                    return o.create(globalState);
                } catch (Exception e1) {
                    throw new AssertionError(e1);
                }
            }).collect(Collectors.toList()), globalState);
        }
    }

    // QPG 相关方法
    @Override
    protected double[] initializeWeightedAverageReward() {
        return new double[] { 1.0 };
    }

    @Override
    protected String getQueryPlan(String selectStr, DerbyGlobalState globalState) throws Exception {
        var explainQuery = "EXPLAIN " + selectStr;
        try {
            var query = new SQLQueryAdapter(explainQuery);
            var resultSet = globalState.executeStatementAndGetAsResultSet(query);

            var planBuilder = new StringBuilder();
            while (resultSet.next()) {
                planBuilder.append(resultSet.getString(1)).append("\n");
            }

            return planBuilder.toString().trim();
        } catch (Exception e) {
            // 新增：记录DQL错误
            globalState.getLoggerNew().logException(e, explainQuery);
            return "";
        }
    }

    @Override
    protected void executeMutator(int index, DerbyGlobalState globalState) throws Exception {
        try {
            var schema = globalState.getSchema();
            if (!schema.getDatabaseTables().isEmpty()) {
                var table = schema.getRandomTable();
                var column = table.getRandomColumn();

                var updateQuery = String.format(
                        "UPDATE %s SET %s = %s WHERE RAND() < 0.1",
                        table.getName(),
                        column.getName(),
                        globalState.getRandomly().getInteger()
                );

                globalState.executeSQL(updateQuery);
            }
        } catch (Exception e) {
            // 新增：记录异常到文件
            globalState.getLoggerNew().logException(e);
            throw new IgnoreMeException();
        }
    }

    @Override
    protected boolean addRowsToAllTables(DerbyGlobalState globalState) throws Exception {
        boolean addedRows = false;
        var schema = globalState.getSchema();

        for (var table : schema.getDatabaseTables()) {
            if (table.getNrRows(globalState) == 0) {
                for (int i = 0; i < 5; i++) {
                    var insertQuery = DerbyInsertGenerator.generateInsert(globalState);
                    try {
                        globalState.executeSQL(insertQuery);
                        addedRows = true;
                    } catch (Exception e) {
                        // 新增：记录DQL错误
                        globalState.getLoggerNew().logException(e, insertQuery);
                        // 继续尝试其他表
                    }
                }
            }
        }

        return addedRows;
    }

    @Override
    public DerbyConnection createDatabase(DerbyGlobalState globalState) throws Exception {
        return null;
    }

    @Override
    public String getDBMSName() {
        return "derby";
    }

    @Override
    public LoggableFactory getLoggableFactory() {
        return new SQLLoggableFactory();
    }
}