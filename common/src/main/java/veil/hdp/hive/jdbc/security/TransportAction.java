package veil.hdp.hive.jdbc.security;

import org.apache.thrift.transport.TTransport;

import java.security.PrivilegedExceptionAction;

public class TransportAction implements PrivilegedExceptionAction<Void> {

    private final TTransport transport;

    public TransportAction(TTransport transport) {
        this.transport = transport;
    }

    @Override
    public Void run() throws Exception {
        transport.open();

        return null;
    }
}
