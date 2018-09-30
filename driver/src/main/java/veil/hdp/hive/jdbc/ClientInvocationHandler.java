/*
 *    Copyright 2018 Timothy J Veil
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package veil.hdp.hive.jdbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

        lock.lock();

        StopWatch sw = new StopWatch(method.getName());
        sw.start();

        try {
            return method.invoke(client, args);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        } finally {

            sw.stop();
            log.trace(sw.shortSummary());

            lock.unlock();

        }
    }
}
