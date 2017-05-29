package veil.hdp.hive.jdbc.utils;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.CookieStore;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.*;

import java.io.IOException;
import java.util.Properties;

public class HttpUtils {

    private static final Logger log = LoggerFactory.getLogger(HttpUtils.class);

    public static CloseableHttpClient buildClient(Properties properties) throws HiveSQLException {

        AuthenticationMode authenticationMode = AuthenticationMode.valueOf(HiveDriverProperty.AUTHENTICATION_MODE.get(properties));

        HttpRequestInterceptor httpRequestInterceptor = null;

        try {
            switch (authenticationMode) {

                case NONE:
                    httpRequestInterceptor = buildBasicInterceptor(properties);
                    break;
                case KERBEROS:
                    httpRequestInterceptor = buildKerberosInterceptor(properties);
                    break;
            }
        } catch (HiveException e) {
            throw new HiveSQLException(e);
        }


        HttpClientBuilder clientBuilder = HttpClientBuilder.create();

        clientBuilder.addInterceptorFirst(httpRequestInterceptor);
        clientBuilder.addInterceptorLast(new XsrfRequestInterceptor());

        return clientBuilder.build();
    }

    private static HttpRequestInterceptor buildKerberosInterceptor(Properties properties) {

        boolean isCookieReplayEnabled = HiveDriverProperty.HTTP_COOKIE_REPLAY_ENABLED.getBoolean(properties);

        if (log.isDebugEnabled()) {
            log.debug("Cookie Replay is enabled!");
        }

        CookieStore cookieStore = null;

        if (isCookieReplayEnabled) {
            cookieStore = new BasicCookieStore();
        }

        return new KerberosRequestInterceptor(properties, cookieStore);
    }

    private static HttpRequestInterceptor buildBasicInterceptor(Properties properties) {

        return new HttpRequestInterceptor() {
            @Override
            public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
                request.addHeader(new BasicScheme().authenticate(new AnonymousCredentials(), request, context));
            }
        };

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
