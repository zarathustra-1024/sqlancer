package sqlancer.derby;

import sqlancer.SQLConnection;
import sqlancer.SQLancerDBConnection;
import sqlancer.common.query.SQLancerResultSet;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static java.sql.DriverManager.getConnection;

public class DerbyConnection extends SQLConnection implements SQLancerDBConnection {

    public DerbyConnection(Connection connection) {
        super(connection);
    }


    public void executeStatement(String query) throws SQLException {
        try (Statement stmt = createStatement()) {
            stmt.execute(query);
        }
    }


    public SQLancerResultSet executeStatementAndGet(String query) throws SQLException {
        Statement stmt = createStatement();
        try {
            ResultSet resultSet = stmt.executeQuery(query);
            return new SQLancerResultSet(resultSet);
        } catch (SQLException e) {
            stmt.close();
            throw e;
        }
    }

    public List<List<Object>> executeAndGet(String query) throws SQLException {
        List<List<Object>> results = new ArrayList<>();

        try (Statement stmt = createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            int columnCount = rs.getMetaData().getColumnCount();

            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(rs.getObject(i));
                }
                results.add(row);
            }
        }

        return results;
    }

    @Override
    public String getDatabaseVersion() throws SQLException {
        try {
            DatabaseMetaData metaData = this.getConnection().getMetaData();
            String version = metaData.getDatabaseProductVersion();
            return "Apache Derby " + (version != null ? version : "Unknown Version");
        } catch (SQLException e) {
            System.err.println("Failed to get version from metadata: " + e.getMessage());
            return "Apache Derby";
        }
    }

    public void executeBatch(List<String> queries) throws SQLException {
        try (Statement stmt = createStatement()) {
            for (String query : queries) {
                stmt.addBatch(query);
            }
            stmt.executeBatch();
        }
    }

    public boolean isValid() throws SQLException {
        try (Statement stmt = createStatement()) {
            stmt.execute("VALUES 1");
            return true;
        } catch (SQLException e) {
            return false;
        }
    }
}
