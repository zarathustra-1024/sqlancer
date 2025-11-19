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
//            return new DerbyNoRECOracle(globalState)
            return null;
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            // NoREC Oracle 不要求所有表都必须有数据
            return false;
        }
    };

    @Override
    public abstract TestOracle<DerbyGlobalState> create(DerbyGlobalState globalState) throws Exception;

    @Override
    public abstract boolean requiresAllTablesToContainRows();
}
