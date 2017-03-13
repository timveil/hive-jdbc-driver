package veil.hdp.hive.jdbc.utils;

import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpVersion;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.HiveDriverIntProperty;
import veil.hdp.hive.jdbc.HiveDriverStringProperty;

import javax.security.sasl.SaslException;
import java.io.IOException;
import java.util.Properties;


public class ThriftUtils {

    private static final Logger log = LoggerFactory.getLogger(ThriftUtils.class);

    public static void openTransport(TTransport transport) throws TTransportException {

        if (!transport.isOpen()) {
            transport.open();
        }

    }

    public static void closeTransport(TTransport transport) {

        if (transport.isOpen()) {
            transport.close();
        }

    }

    public static TCLIService.Client createClient(TTransport transport) {
        return new TCLIService.Client(new TBinaryProtocol(transport));
    }

    // lets move all http stuff to different class
    public static TTransport createHttpTransport(Properties properties) throws TTransportException {
        String user = properties.getProperty(HiveDriverStringProperty.USER.getName());
        String password = properties.getProperty(HiveDriverStringProperty.PASSWORD.getName());
        String host = properties.getProperty(HiveDriverStringProperty.HOST.getName());
        int port = Integer.parseInt(properties.getProperty(HiveDriverIntProperty.PORT_NUMBER.getName()));

        HttpClientBuilder clientBuilder = HttpClientBuilder.create();
        clientBuilder.addInterceptorFirst(new HttpRequestInterceptor() {
            @Override
            public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
                request.addHeader(new BasicScheme().authenticate(new UsernamePasswordCredentials(user, password), request, context));
            }
        });

        //todo: don't think this client is being properly closed
        CloseableHttpClient client = clientBuilder.build();

        return new THttpClient("http://" + host + ":" + port + "/cliservice", client);

    }

    public static TTransport createBinaryTransport(Properties properties, int loginTimeoutMilliseconds) throws SaslException {
        // no support for no-sasl
        // no support for delegation tokens or ssl yet

        String user = properties.getProperty(HiveDriverStringProperty.USER.getName());
        String password = properties.getProperty(HiveDriverStringProperty.PASSWORD.getName());
        String host = properties.getProperty(HiveDriverStringProperty.HOST.getName());
        int port = Integer.parseInt(properties.getProperty(HiveDriverIntProperty.PORT_NUMBER.getName()));

        TTransport socketTransport = HiveAuthFactory.getSocketTransport(host, port, loginTimeoutMilliseconds);

        // hack: password can't be empty.  must always specify a non-null, non-empty string
        return PlainSaslHelper.getPlainTransport(user, password, socketTransport);

    }

}
