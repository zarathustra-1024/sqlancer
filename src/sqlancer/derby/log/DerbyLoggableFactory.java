package sqlancer.derby.log;

import sqlancer.common.log.LoggableFactory;


public class DerbyLoggableFactory {
    public void logQuery(String query) {
        System.out.println("[DERBY QUERY] " + query);
    }

    public void logCurrentState(String state) {
        System.out.println("[DERBY STATE] " + state);
    }

    public void logException(Exception e) {
        System.out.println("[DERBY ERROR] " + e.getMessage());
    }

    public void logInfo(String info) {
        System.out.println("[DERBY INFO] " + info);
    }

    public void logTime(String time) {
        System.out.println(" -- " + time + " ms");
    }
}