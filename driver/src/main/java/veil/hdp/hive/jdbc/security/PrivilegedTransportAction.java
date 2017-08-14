package veil.hdp.hive.jdbc.security;

import org.apache.thrift.transport.TTransport;

import java.security.PrivilegedExceptionAction;

class PrivilegedTransportAction implements PrivilegedExceptionAction<Void> {

    private final TTransport transport;

    PrivilegedTransportAction(TTransport transport) {
        this.transport = transport;
    }

    @Override
    public Void run() throws Exception {
        transport.open();

        return null;
    }
}
