package veil.hdp.hive.jdbc.core.security;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import veil.hdp.hive.jdbc.thrift.WrappedTransport;

import javax.security.auth.Subject;
import java.security.PrivilegedActionException;

public class JaasTransport extends WrappedTransport {

    private final Subject subject;

    public JaasTransport(TTransport wrapped, Subject subject) {
        super(wrapped);
        this.subject = subject;
    }

    @Override
    public void open() throws TTransportException {

        try {
            Subject.doAs(subject, new PrivilegedTransportAction(wrapped));
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e);
        }
    }
}
