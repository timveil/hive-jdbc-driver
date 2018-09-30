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

package veil.hdp.hive.jdbc.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import veil.hdp.hive.jdbc.ClientInvocationHandler;
import veil.hdp.hive.jdbc.HiveDriverProperty;
import veil.hdp.hive.jdbc.HiveException;
import veil.hdp.hive.jdbc.bindings.*;
import veil.hdp.hive.jdbc.bindings.TCLIService.Client;
import veil.hdp.hive.jdbc.thrift.HiveThriftException;
import veil.hdp.hive.jdbc.thrift.InvalidProtocolException;
import veil.hdp.hive.jdbc.thrift.ThriftTransport;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.*;


public final class ThriftUtils {

    private static final Logger log = LogManager.getLogger(ThriftUtils.class);


    private ThriftUtils() {
    }

    public static void openTransport(TTransport transport, int timeout) {

        ExecutorService executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "open-thrift-transport-thread"));

        try {

            Future future = executor.submit(() -> {
                try {
                    transport.open();
                } catch (TTransportException e) {
                    throw new HiveException(e);
                }
            });

            future.get(timeout, TimeUnit.MILLISECONDS);

        } catch (InterruptedException | ExecutionException e) {
            throw new HiveException(e);
        } catch (TimeoutException e) {
            throw new HiveException("The Thrift Transport did not open prior to Timeout.  If using Kerberos, double check that you have a valid client Principal by running klist.", e);
        } finally {
            executor.shutdown();
        }

    }


    public static TCLIService.Iface createClient(ThriftTransport transport) {
        TCLIService.Iface client = new Client(new TBinaryProtocol(transport.getTransport()));

        return (TCLIService.Iface) Proxy.newProxyInstance(ThriftUtils.class.getClassLoader(), new Class[]{TCLIService.Iface.class}, new ClientInvocationHandler(client));
    }


    public static TOpenSessionResp openSession(Properties properties, TCLIService.Iface client, TProtocolVersion protocolVersion) throws InvalidProtocolException {


        TOpenSessionReq openSessionReq = new TOpenSessionReq(protocolVersion);

        // set properties for session
        Map<String, String> configuration = buildSessionConfig(properties);

        openSessionReq.setConfiguration(configuration);

        try {
            TOpenSessionResp resp = client.OpenSession(openSessionReq);

            ThriftUtils.checkStatus(resp.getStatus());

            return resp;
        } catch (TException e) {
            if (StringUtils.containsIgnoreCase(e.getMessage(), "'client_protocol' is unset")) {
                // this often happens when the protocol of the driver is higher than supported by the server.  in other words, driver is newer than server.  handle this gracefully.
                throw new InvalidProtocolException(e);
            } else {
                throw new HiveThriftException(e);
            }
        }

    }


    private static Map<String, String> buildSessionConfig(Properties properties) {
        Set<String> propertyNames = properties.stringPropertyNames();

        Map<String, String> openSessionConfig = new HashMap<>(propertyNames.size() + 1);

        for (String property : propertyNames) {
            // no longer going to use HiveConf.ConfVars to validate properties.  it requires too many dependencies.  let server side deal with this.
            if (property.startsWith("hive.")) {
                openSessionConfig.put("set:hiveconf:" + property, properties.getProperty(property));
            }
        }

        openSessionConfig.put("use:database", HiveDriverProperty.DATABASE_NAME.get(properties));

        return openSessionConfig;
    }


    public static void checkStatus(TStatus status) {

        TStatusCode statusCode = status.getStatusCode();

        if (statusCode == TStatusCode.SUCCESS_STATUS || statusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS) {
            return;
        }

        throw new HiveThriftException(status);
    }

}
