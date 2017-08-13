package veil.hdp.hive.jdbc.thrift;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class WrappedTransport extends TTransport {

    protected final TTransport wrapped;

    protected WrappedTransport(TTransport wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public boolean isOpen() {
        return wrapped.isOpen();
    }

    @Override
    public void open() throws TTransportException {
        wrapped.open();
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
        return wrapped.read(buf, off, len);
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        wrapped.write(buf, off, len);
    }

    @Override
    public boolean peek() {
        return wrapped.peek();
    }

    @Override
    public int readAll(byte[] buf, int off, int len) throws TTransportException {
        return wrapped.readAll(buf, off, len);
    }

    @Override
    public void write(byte[] buf) throws TTransportException {
        wrapped.write(buf);
    }

    @Override
    public void flush() throws TTransportException {
        wrapped.flush();
    }

    @Override
    public byte[] getBuffer() {
        return wrapped.getBuffer();
    }

    @Override
    public int getBufferPosition() {
        return wrapped.getBufferPosition();
    }

    @Override
    public int getBytesRemainingInBuffer() {
        return wrapped.getBytesRemainingInBuffer();
    }

    @Override
    public void consumeBuffer(int len) {
        wrapped.consumeBuffer(len);
    }
}
