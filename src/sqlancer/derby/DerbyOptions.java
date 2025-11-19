package sqlancer.derby;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

/**
 * Derby 数据库配置选项
 * 使用 JCommander 注解定义命令行参数
 */
@Parameters(separators = "=", commandDescription = "Apache Derby (embedded database)")
public class DerbyOptions implements DBMSSpecificOptions<DerbyOracleFactory> {

    @Parameter(names = "--derby-db-path", description = "Path to Derby database (memory:dbname for memory db)")
    public String dbPath = "memory:derby_test";

    @Parameter(names = "--derby-oracle", description = "Test oracle to use")
    public List<DerbyOracleFactory> oracle = Arrays.asList(DerbyOracleFactory.NOREC); // 这里是扩展的入口

    @Override
    public List<DerbyOracleFactory> getTestOracleFactory() {
        return oracle;
    }

    public String getDbPath() {
        return dbPath;
    }
}