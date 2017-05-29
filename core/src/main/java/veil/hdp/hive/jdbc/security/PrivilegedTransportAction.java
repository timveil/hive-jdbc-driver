package veil.hdp.hive.jdbc.security;

import org.apache.thrift.transport.TTransport;

import java.security.PrivilegedExceptionAction;

public class PrivilegedTransportAction implements PrivilegedExceptionAction<Void> {

    private final TTransport transport;

    public PrivilegedTransportAction(TTransport transport) {
        this.transport = transport;
    }

    @Override
    public Void run() throws Exception {
        transport.open();

        return null;
    }
}
