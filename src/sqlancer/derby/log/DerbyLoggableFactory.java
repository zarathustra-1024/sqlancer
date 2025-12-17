package sqlancer.derby.log;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Derby 日志工厂 - 支持控制台输出和错误日志文件输出
 */
public class DerbyLoggableFactory {

    private PrintWriter errorFileWriter = null;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 初始化错误日志文件
     */
    public void initErrorLogFile(String errorLogFilePath) {
        if (errorLogFilePath != null && !errorLogFilePath.trim().isEmpty()) {
            try {
                // 创建文件写入器（追加模式）
                FileWriter fw = new FileWriter(errorLogFilePath, true);
                errorFileWriter = new PrintWriter(fw, true);

                // 写入文件头
                writeToErrorFile("========================================");
                writeToErrorFile("Derby SQLancer Error Log");
                writeToErrorFile("Start time: " + dateFormat.format(new Date()));
                writeToErrorFile("========================================");

                System.out.println("[DERBY INFO] Error log file initialized: " + errorLogFilePath);
            } catch (IOException e) {
                System.err.println("[DERBY ERROR] Failed to initialize error log file: " + errorLogFilePath);
                System.err.println("[DERBY ERROR] Cause: " + e.getMessage());
                errorFileWriter = null;
            }
        }
    }

    /**
     * 写入错误日志文件
     */
    private void writeToErrorFile(String message) {
        if (errorFileWriter != null) {
            String timestamp = dateFormat.format(new Date());
            errorFileWriter.println("[" + timestamp + "] " + message);
            errorFileWriter.flush();
        }
    }

    /**
     * 记录查询（控制台输出，不写入文件）
     */
    public void logQuery(String query) {
        System.out.println("[DERBY QUERY] " + query);
    }

    /**
     * 记录当前状态（控制台输出，不写入文件）
     */
    public void logCurrentState(String state) {
        System.out.println("[DERBY STATE] " + state);
    }

    /**
     * 记录异常（同时输出到控制台和错误日志文件）
     */
    public void logException(Exception e) {
        String errorMsg = e.getMessage() != null ? e.getMessage() : e.toString();

        // 控制台输出
        System.err.println("[DERBY ERROR] " + errorMsg);

        // 写入错误日志文件
        writeToErrorFile("[ERROR] " + errorMsg);

        // 如果启用了文件日志，记录堆栈跟踪（前5行）
        if (errorFileWriter != null) {
            errorFileWriter.println("Stack trace:");
            StackTraceElement[] stackTrace = e.getStackTrace();
            int limit = Math.min(stackTrace.length, 5);
            for (int i = 0; i < limit; i++) {
                errorFileWriter.println("  " + stackTrace[i]);
            }
            errorFileWriter.println();
        }
    }

    /**
     * 记录信息（控制台输出，不写入文件）
     */
    public void logInfo(String info) {
        System.out.println("[DERBY INFO] " + info);
    }

    /**
     * 记录时间（控制台输出，不写入文件）
     */
    public void logTime(String time) {
        System.out.println(" -- " + time + " ms");
    }

    /**
     * 记录错误（同时输出到控制台和错误日志文件）
     */
    public void logError(String error) {
        // 控制台输出
        System.err.println("[DERBY ERROR] " + error);

        // 写入错误日志文件
        writeToErrorFile("[ERROR] " + error);
    }

    /**
     * 关闭错误日志文件
     */
    public void closeErrorLog() {
        if (errorFileWriter != null) {
            writeToErrorFile("========================================");
            writeToErrorFile("Log end time: " + dateFormat.format(new Date()));
            writeToErrorFile("========================================");
            errorFileWriter.close();
            errorFileWriter = null;
            System.out.println("[DERBY INFO] Error log file closed");
        }
    }
}