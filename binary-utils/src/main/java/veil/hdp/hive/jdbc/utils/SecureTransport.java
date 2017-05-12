package veil.hdp.hive.jdbc.utils;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import veil.hdp.hive.jdbc.thrift.WrappedTransport;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class SecureTransport extends WrappedTransport {

    private UserGroupInformation ugi;

    public SecureTransport(TTransport wrapped, UserGroupInformation ugi) {
        super(wrapped);
        this.ugi = ugi;
    }

    @Override
    public void open() throws TTransportException {

        try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() {
                    try {
                        wrapped.open();
                    } catch (TTransportException e) {
                        throw new RuntimeException(e);
                    }

                    return null;
                }
            });
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
