package veil.hdp.hive.jdbc.security;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import veil.hdp.hive.jdbc.thrift.WrappedTransport;

import javax.security.auth.Subject;
import java.security.PrivilegedActionException;

public class SecureTransport extends WrappedTransport {

    private final Subject subject;

    public SecureTransport(TTransport wrapped, Subject subject) {
        super(wrapped);
        this.subject = subject;
    }

    @Override
    public void open() throws TTransportException {

        try {
            Subject.doAs(subject, new TransportAction(wrapped));
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e);
        }
    }
}
