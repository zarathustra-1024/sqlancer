package sqlancer.derby;

import sqlancer.common.oracle.TestOracle;
import sqlancer.derby.oracle.DerbyNoRECOracle;
import sqlancer.OracleFactory;
/**
 * Derby 测试 Oracle 工厂枚举
 * 负责创建不同类型的测试 Oracle
 */
public enum DerbyOracleFactory implements OracleFactory<DerbyGlobalState> {
    NOREC {
        @Override
        public TestOracle<DerbyGlobalState> create(DerbyGlobalState globalState) throws Exception {
            return new DerbyNoRECOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return false;
        }
    };

    @Override
    public abstract TestOracle<DerbyGlobalState> create(DerbyGlobalState globalState) throws Exception;

    @Override
    public abstract boolean requiresAllTablesToContainRows();
}