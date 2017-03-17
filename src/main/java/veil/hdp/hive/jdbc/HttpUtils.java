package veil.hdp.hive.jdbc;

import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class HttpUtils {

    private static final Logger log = LoggerFactory.getLogger(HttpUtils.class);

    public static CloseableHttpClient buildClient(Properties properties) {
        String user = properties.getProperty(HiveDriverStringProperty.USER.getName());
        String password = properties.getProperty(HiveDriverStringProperty.PASSWORD.getName());


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
}
