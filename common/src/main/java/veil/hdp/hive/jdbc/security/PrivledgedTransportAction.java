package veil.hdp.hive.jdbc.security;

import org.apache.thrift.transport.TTransport;

import java.security.PrivilegedExceptionAction;

public class PrivledgedTransportAction implements PrivilegedExceptionAction<Void> {

    private final TTransport transport;

    public PrivledgedTransportAction(TTransport transport) {
        this.transport = transport;
    }

    @Override
    public Void run() throws Exception {
        transport.open();

        return null;
    }
}
