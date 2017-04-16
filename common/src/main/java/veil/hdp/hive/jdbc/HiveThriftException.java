package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TStatus;
import org.apache.thrift.TException;

import java.sql.SQLException;
import java.util.List;

public class HiveThriftException extends SQLException {

    public HiveThriftException(TStatus status) {

        super(status.getErrorMessage(), status.getSqlState(), status.getErrorCode());

        if (status.getInfoMessages() != null) {
            initCause(toStackTrace(status.getInfoMessages(), null, 0));
        }

    }

    public HiveThriftException(TException exception) {
        super(exception);
    }

    //todo: need to rewrite and test; this is ugly
    private static Throwable toStackTrace(List<String> details, StackTraceElement[] parent, int index) {
        String detail = details.get(index++);
        if (!detail.startsWith("*")) {
            return null;  // should not be happened. ignore remaining
        }
        int i1 = detail.indexOf(':');
        int i3 = detail.lastIndexOf(':');
        int i2 = detail.substring(0, i3).lastIndexOf(':');
        String exceptionClass = detail.substring(1, i1);
        String exceptionMessage = detail.substring(i1 + 1, i2);
        Throwable ex = newInstance(exceptionClass, exceptionMessage);

        Integer length = Integer.valueOf(detail.substring(i2 + 1, i3));
        Integer unique = Integer.valueOf(detail.substring(i3 + 1));

        int i = 0;
        StackTraceElement[] trace = new StackTraceElement[length];
        for (; i <= unique; i++) {
            detail = details.get(index++);
            int j1 = detail.indexOf(':');
            int j3 = detail.lastIndexOf(':');
            int j2 = detail.substring(0, j3).lastIndexOf(':');
            String className = detail.substring(0, j1);
            String methodName = detail.substring(j1 + 1, j2);
            String fileName = detail.substring(j2 + 1, j3);
            if (fileName.isEmpty()) {
                fileName = null;
            }
            int lineNumber = Integer.valueOf(detail.substring(j3 + 1));
            trace[i] = new StackTraceElement(className, methodName, fileName, lineNumber);
        }
        int common = trace.length - i;
        if (common > 0) {
            System.arraycopy(parent, parent.length - common, trace, trace.length - common, common);
        }
        if (details.size() > index) {
            ex.initCause(toStackTrace(details, trace, index));
        }
        ex.setStackTrace(trace);
        return ex;
    }


    private static Throwable newInstance(String className, String message) {
        try {
            return (Throwable) Class.forName(className).getConstructor(String.class).newInstance(message);
        } catch (Exception e) {
            return new RuntimeException(className + ':' + message);
        }
    }
}
