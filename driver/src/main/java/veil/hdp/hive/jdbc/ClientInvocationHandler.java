package veil.hdp.hive.jdbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import veil.hdp.hive.jdbc.bindings.TCLIService;
import veil.hdp.hive.jdbc.utils.StopWatch;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.locks.ReentrantLock;

public class ClientInvocationHandler implements InvocationHandler {

    private static final Logger log = LogManager.getLogger(ClientInvocationHandler.class);

    private final TCLIService.Iface client;
    private final ReentrantLock lock = new ReentrantLock(true);

    public ClientInvocationHandler(TCLIService.Iface client) {
        this.client = client;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        StopWatch sw = new StopWatch(method.getName());
        sw.start();
        lock.lock();
        try {
            return method.invoke(client, args);
        } catch (InvocationTargetException e) {
            Throwable targetException = e.getTargetException();

            if (targetException instanceof TException) {
                throw targetException;
            } else {
                throw new TException("Error in calling method " + method.getName(), targetException);
            }

        } catch (Exception e) {
            throw new TException("Error in calling method " + method.getName(), e);
        } finally {
            lock.unlock();
            sw.stop();
            log.debug(sw.shortSummary());
        }
    }
}
