package veil.hdp.hive.jdbc.security;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import veil.hdp.hive.jdbc.thrift.WrappedTransport;

import javax.security.auth.Subject;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedActionException;

public class PreAuthenticatedSecureTransport extends WrappedTransport {

    public PreAuthenticatedSecureTransport(TTransport wrapped) {
        super(wrapped);
    }

    @Override
    public void open() throws TTransportException {

        try {
            AccessControlContext context = AccessController.getContext();
            Subject subject = Subject.getSubject(context);
            Subject.doAs(subject, new TransportAction(wrapped));
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e);
        }
    }
}
