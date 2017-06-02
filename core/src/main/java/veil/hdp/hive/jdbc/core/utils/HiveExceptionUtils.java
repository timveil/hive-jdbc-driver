package veil.hdp.hive.jdbc.core.utils;


import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import veil.hdp.hive.jdbc.core.HiveSQLException;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HiveExceptionUtils {

    public static Throwable toStackTrace(List<String> details) {

        List<ExceptionDetails> list = new ArrayList<>();

        for (String detail : details) {

            // this is a result of the grotesque HiveSQLException.enroll; first line of stack starts with * and ends with XX:XX
            if (detail.startsWith("*")) {

                detail = StringUtils.stripStart(detail, "*");
                detail = StringUtils.removePattern(detail, ":[0-9]+:[0-9]+$");

                int firstColon = detail.indexOf(':');
                String exceptionClass = detail.substring(0, firstColon);
                String exceptionMessage = detail.substring(firstColon + 1);

                list.add(new ExceptionDetails(exceptionClass, exceptionMessage));

            } else {
                List<String> lineItems = Splitter.on(':').splitToList(detail);

                ExceptionDetails exd = Iterables.getLast(list);

                exd.add(new StackTraceElement(lineItems.get(0), lineItems.get(1), lineItems.get(2), Integer.parseInt(lineItems.get(3))));
            }

        }

        Throwable throwable = null;

        Collections.reverse(list);

        for (ExceptionDetails exd : list) {
            if (throwable == null) {
                throwable = exd.build(null);
            } else {
                throwable = exd.build(throwable);
            }
        }

        return throwable;
    }

    private static Throwable newInstance(String className, String message, Throwable cause) {
        try {
            return (Throwable) Class.forName(className).getConstructor(String.class, Throwable.class).newInstance(message, cause);
        } catch (Exception e) {
            return new HiveSQLException(MessageFormat.format("Original Exception Class [{0}], Original Exception Message [{1}]", className, message), cause);
        }
    }

    private static class ExceptionDetails {
        private final String exceptionClass;
        private final String exceptionMessage;
        private final List<StackTraceElement> stackTraceElements;

        ExceptionDetails(String exceptionClass, String exceptionMessage) {
            this.exceptionClass = exceptionClass;
            this.exceptionMessage = exceptionMessage;
            this.stackTraceElements = new ArrayList<>();
        }

        void add(StackTraceElement element) {
            stackTraceElements.add(element);
        }

        Throwable build(Throwable cause) {

            Throwable throwable = newInstance(exceptionClass, exceptionMessage, cause);
            throwable.setStackTrace(stackTraceElements.toArray(new StackTraceElement[stackTraceElements.size()]));

            return throwable;
        }
    }
}
