package veil.hdp.hive.jdbc.core.security.http;


import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.util.Properties;

public class BasicRequestInterceptor implements HttpRequestInterceptor {

    private final Properties properties;

    public BasicRequestInterceptor(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void process(HttpRequest httpRequest, HttpContext httpContext) throws HttpException, IOException {
        httpRequest.addHeader(new BasicScheme().authenticate(new AnonymousCredentials(), httpRequest, httpContext));
    }
}
