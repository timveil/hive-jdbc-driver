package veil.hdp.hive.jdbc.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.CookieStore;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.AuthenticationMode;
import veil.hdp.hive.jdbc.HiveDriverProperty;
import veil.hdp.hive.jdbc.HiveException;
import veil.hdp.hive.jdbc.security.http.BasicRequestInterceptor;
import veil.hdp.hive.jdbc.security.http.KerberosRequestInterceptor;
import veil.hdp.hive.jdbc.security.http.XsrfRequestInterceptor;
import veil.hdp.hive.jdbc.thrift.HiveThriftException;

import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.Properties;

public class HttpUtils {

    private static final Logger log = LoggerFactory.getLogger(HttpUtils.class);

    private static final String HTTP = "http";
    private static final String HTTPS = "https";

    /*
        todo: see if any of these optimizations can/should be applied

        From Thrift THttpClient

        * When using HttpClient, the following configuration leads to 5-15%
        * better performance than the HttpURLConnection implementation:
        *
        * http.protocol.version=HttpVersion.HTTP_1_1
        * http.protocol.content-charset=UTF-8
        * http.protocol.expect-continue=false
        * http.connection.stalecheck=false
     */

    public static CloseableHttpClient buildClient(Properties properties) {

        AuthenticationMode authenticationMode = AuthenticationMode.valueOf(HiveDriverProperty.AUTHENTICATION_MODE.get(properties));

        HttpRequestInterceptor httpRequestInterceptor = null;

        switch (authenticationMode) {

            case NONE:
                httpRequestInterceptor = buildBasicInterceptor(properties);
                break;
            case KERBEROS:
                httpRequestInterceptor = buildKerberosInterceptor(properties);
                break;
        }

        if (httpRequestInterceptor == null) {
            throw new HiveException("Authentication Mode [" + authenticationMode + "] is not supported when creating an HTTP Client!");
        }

        Registry<ConnectionSocketFactory> registry = buildConnectionSocketFactoryRegistry(properties);

        HttpClientBuilder clientBuilder = HttpClients.custom();

        if (HiveDriverProperty.HTTP_POOL_ENABLED.getBoolean(properties)) {
            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(registry);
            cm.setMaxTotal(HiveDriverProperty.HTTP_POOL_MAX_TOTAL.getInt(properties));
            cm.setDefaultMaxPerRoute(HiveDriverProperty.HTTP_POOL_MAX_PER_ROUTE.getInt(properties));

            clientBuilder.setConnectionManager(cm);
        } else {
            BasicHttpClientConnectionManager cm = new BasicHttpClientConnectionManager(registry);

            clientBuilder.setConnectionManager(cm);
        }

        clientBuilder.addInterceptorFirst(httpRequestInterceptor);
        clientBuilder.addInterceptorLast(new XsrfRequestInterceptor());

        return clientBuilder.build();
    }

    private static Registry<ConnectionSocketFactory> buildConnectionSocketFactoryRegistry(Properties properties) {

        RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
        registryBuilder.register(HTTP, PlainConnectionSocketFactory.getSocketFactory());


        if (HiveDriverProperty.SSL_ENABLED.getBoolean(properties)) {
            SSLConnectionSocketFactory sslSocketFactory;

            if (HiveDriverProperty.SSL_TWO_WAY_ENABLED.getBoolean(properties)) {
                sslSocketFactory = buildTwoWaySSLSocketFactory(properties);
            } else if (HiveDriverProperty.SSL_TRUST_STORE_PATH.hasValue(properties)) {
                sslSocketFactory = buildOneWaySSLSocketFactory(properties);
            } else {
                sslSocketFactory = buildDefaultSSLSocketFactory();
            }

            registryBuilder.register(HTTPS, sslSocketFactory);

        }

        return registryBuilder.build();
    }

    private static SSLConnectionSocketFactory buildDefaultSSLSocketFactory() {
        return SSLConnectionSocketFactory.getSocketFactory();
    }

    private static SSLConnectionSocketFactory buildOneWaySSLSocketFactory(Properties properties) {

        try {
            char[] trustStorePassword = HiveDriverProperty.SSL_TRUST_STORE_PASSWORD.get(properties).toCharArray();

            KeyStore trustStore = buildKeyStore(HiveDriverProperty.SSL_TRUST_STORE_PATH.get(properties), HiveDriverProperty.SSL_TRUST_STORE_TYPE.get(properties), trustStorePassword);

            SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(trustStore, null).build();

            return new SSLConnectionSocketFactory(sslContext);

        } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
            throw new HiveException(e);
        }
    }


    private static SSLConnectionSocketFactory buildTwoWaySSLSocketFactory(Properties properties)  {

        try {
            char[] trustStorePassword = HiveDriverProperty.SSL_TRUST_STORE_PASSWORD.get(properties).toCharArray();

            KeyStore trustStore = buildKeyStore(HiveDriverProperty.SSL_TRUST_STORE_PATH.get(properties), HiveDriverProperty.SSL_TRUST_STORE_TYPE.get(properties), trustStorePassword);

            char[] keystorePassword = HiveDriverProperty.SSL_KEY_STORE_PASSWORD.get(properties).toCharArray();

            KeyStore keyStore = buildKeyStore(HiveDriverProperty.SSL_KEY_STORE_PATH.get(properties), HiveDriverProperty.SSL_KEY_STORE_TYPE.get(properties), keystorePassword);

            SSLContext sslContext = SSLContexts.custom().loadKeyMaterial(keyStore, keystorePassword).loadTrustMaterial(trustStore, null).build();

            return new SSLConnectionSocketFactory(sslContext);

        } catch (NoSuchAlgorithmException | KeyManagementException | UnrecoverableKeyException | KeyStoreException e) {
            throw new HiveException(e);
        }
    }

    private static KeyStore buildKeyStore(String path, String type, char[] password)  {

        if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("keystore path is null!");
        }

        try {

            KeyStore keyStore = KeyStore.getInstance(type);

            try (FileInputStream fis = new FileInputStream(path)) {
                keyStore.load(fis, password);
            }

            return keyStore;
        } catch (CertificateException | IOException | NoSuchAlgorithmException | KeyStoreException e) {
            throw new HiveException(e);
        }
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
        return new BasicRequestInterceptor(properties);
    }

    public static TTransport createHttpTransport(Properties properties, CloseableHttpClient httpClient) {
        String host = HiveDriverProperty.HOST_NAME.get(properties);
        int port = HiveDriverProperty.PORT_NUMBER.getInt(properties);
        boolean sslEnabled = HiveDriverProperty.SSL_ENABLED.getBoolean(properties);
        String endpoint = HiveDriverProperty.HTTP_ENDPOINT.get(properties);

        String scheme = sslEnabled ? HTTPS : HTTP;

        try {
            return new THttpClient(scheme + "://" + host + ':' + port + '/' + endpoint, httpClient);
        } catch (TTransportException e) {
            throw new HiveThriftException(e);
        }

    }
}
