package veil.hdp.hive.jdbc.utils;

import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.HiveDriverProperty;
import veil.hdp.hive.jdbc.HiveThriftException;

import java.io.IOException;
import java.util.Properties;

public class HttpUtils {

    private static final Logger log = LoggerFactory.getLogger(HttpUtils.class);

    public static CloseableHttpClient buildClient(Properties properties) {

        // todo: is this still required, because its not on the binary side when AuthenticationMode = NONE
        String user = HiveDriverProperty.USER.get(properties);
        String password = HiveDriverProperty.PASSWORD.get(properties);


        HttpClientBuilder clientBuilder = HttpClientBuilder.create();
        clientBuilder.addInterceptorFirst((HttpRequestInterceptor) (request, context) -> request.addHeader(new BasicScheme().authenticate(new UsernamePasswordCredentials(user, password), request, context)));

        return clientBuilder.build();
    }

    public static void closeClient(CloseableHttpClient client) {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                log.warn(e.getMessage(), e);
            }
        }
    }


    public static TTransport createHttpTransport(Properties properties, CloseableHttpClient httpClient) {
        String host = HiveDriverProperty.HOST_NAME.get(properties);
        int port = HiveDriverProperty.PORT_NUMBER.getInt(properties);
        boolean sslEnabled = HiveDriverProperty.HTTP_SSL_ENABLED.getBoolean(properties);
        String endpoint = HiveDriverProperty.HTTP_ENDPOINT.get(properties);

        String scheme = sslEnabled ? "https" : "http";

        try {
            return new THttpClient(scheme + "://" + host + ':' + port + '/' + endpoint, httpClient);
        } catch (TTransportException e) {
            throw new HiveThriftException(e);
        }

    }
}
